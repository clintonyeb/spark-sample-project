import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

case class PopulationTable(Population: Double, UnEmployedMean: Double, UnEmployedVariance: Double, EmployedMean: Double, EmployedVariance: Double)

object Bootstrap extends Task {
  override def run(args: Array[String]): Unit = {
    val spark = session("Bootstrap App")
    spark.sparkContext.setLogLevel("ERROR")
    val csv = spark.sparkContext.textFile(filePath("arrests.csv"))
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val data = headerAndRows.filter(_ (0) != header(0)).cache()

    // employment and age
    val population = data.map(p => (p(6), p(4))).map(p => (p._1, p._2.toInt)).cache()
    val popGroup = population.groupByKey().map(x => (x._1, (mean(x._2), variance(x._2)))).sortByKey().cache()
    val original:mutable.Map[String, (Double, Double)] = mutable.Map()
    popGroup.collect().foreach(x => original(x._1) = x._2)

    print("Original: ")
    popGroup.collect().foreach(print)
    println()

    def absoluteError(actual: Double, estimate: Double): Double = {
      ((actual - estimate).abs * 100) / actual
    }

    val percentages = List(.05, .15, .25, .35, .45, .55, .65, .75, .85, .95)
    val computed = percentages.map(p => (p, estimate(population, p)))
      .map(e => {
        val p = e._1
        val est = e._2
        PopulationTable(p,
          absoluteError(original(original.keys.head)._1, est(original.keys.head)._1),
          absoluteError(original(original.keys.head)._2, est(original.keys.head)._2),
          absoluteError(original(original.keys.last)._1, est(original.keys.last)._1),
          absoluteError(original(original.keys.last)._2, est(original.keys.last)._2))
      })

    import spark.sqlContext.implicits._

    val df = computed.toDF()

    // display
    showTable(df)
    val graph = showGraph(spark, df)
    val graphsc = spark.sparkContext.parallelize(Seq(graph))
    graphsc.saveAsTextFile(filePath("graph.json"))

    spark.stop()
  }

  def showTable(df: sql.DataFrame): Unit = {
    df.show()
  }

  def showGraph(spark: SparkSession, df: sql.DataFrame): String = {
    import vegas._
    import vegas.sparkExt._

    val plot = Vegas("Spark")
    .withDataFrame(df)
      .mark(Bar) // Change to .mark(Area)
      .encodeX("spark", Nom, sortField= Sort("users count", AggOps.Mean))
    .encodeY("users_count", Quant)

    def renderHTML(): Unit = {
      plot.html.pageHTML() + "\n" + // a complete HTML page containing the plot
      plot.html.frameHTML("plot") // an iframe containing the plot
    }

    def renderWindow(): Unit = {
      plot.window.show
    }

    plot.toJson
  }

  def estimate(population: RDD[(String, Int)], sampleSize: Double): collection.Map[String, (Double, Double)] = {
    def addToCache(items: RDD[(String, (Double, Double))], cache:mutable.Map[String, (Double, Double)]): Unit = {
      items.collect().foreach(item => {
        if (cache.contains(item._1)) {
          val temp = cache.get(item._1)
          cache (item._1) = (temp.get._1 + item._2._1, temp.get._2 + item._2._2)
        }
        else
          cache(item._1) = (item._2._1, item._2._2)
      })
    }

    def resample(popSample: RDD[(String, Int)]): RDD[(String, (Double, Double))] = {
      val newSample = popSample.sample(withReplacement = true, 1)
      newSample.groupByKey().map(x => (x._1, (mean(x._2), variance(x._2))))
    }

    val popSample = population.sample(withReplacement = false, sampleSize).cache()
    val cache:mutable.Map[String, (Double, Double)] = mutable.Map()
    1.to(1000).foreach(_ => addToCache(resample(popSample), cache))
    val avg = cache.mapValues(v =>  (v._1 / 1000, v._2 / 1000))

    print(sampleSize + ": ")
    print(avg)
    println()
    avg
  }

  def sqr(x: Double): Double = x * x

  def mean(in: Iterable[Int]): Double = {
    in.sum / in.count(_ => true)
  }

  def variance(in: Iterable[Int]): Double = {
    val _mean = mean(in)
    in.map(x => sqr(x - _mean)).sum / (in.count(_ => true) - 1)
  }
}
