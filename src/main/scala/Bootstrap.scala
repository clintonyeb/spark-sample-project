import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


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
    val popGroup: RDD[(String, (Double, Double))] = population.groupByKey().map(x => (x._1, (mean(x._2), variance(x._2))))
    val popMap = popGroup.collectAsMap()

    def absoluteError(actual: Double, estimate: Double): Double = {
      ((actual - estimate).abs * 100) / actual
    }

    // saved as [(key, sampleSize), (mean, variance)]
    def retrieveEstimates(): RDD[(String, (Double, Double, Double))] = {
      spark.sparkContext.objectFile(filePath("bootstrap/estimate*/*"))
    }

    val percentages = List(.05, .15, .25, .35, .45, .55, .65, .75, .85, .95)
    //    val percentages = List(.25)
    percentages.foreach(p => (p, estimate(population, p)))

    val estimates: RDD[(String, (Double, Double, Double))] = retrieveEstimates()
    val estimatesGrouped: RDD[((String, Double), (Double, Double))] = estimates
      .map(x => ((x._1, x._2._1), (x._2._2, x._2._3)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => ((x._1._1, x._1._2), (x._2._1 / 1000, x._2._2 / 1000)))

    val estimatesGroupedBySampleSize: RDD[(Double, Iterable[(String, (Double, Double))])] =
      estimatesGrouped.map(x => (x._1._2, (x._1._1, (x._2._1, x._2._2)))).groupByKey()

    val table = estimatesGroupedBySampleSize.map(estimate => {
      val sampleSize: Double = estimate._1
      val value: Iterable[(String, (Double, Double))] = estimate._2
      val map = value.groupBy(x => x._1).mapValues(x => x.head._2)
      val keys = map.keys.toSeq.sorted
      val no = map(keys.head)
      val yes = map(keys.last)

      val popNo = popMap(keys.head)
      val popYes = popMap(keys.last)

      PopulationTable(sampleSize,
        absoluteError(popNo._1, no._1),
        absoluteError(popNo._2, no._2),
        absoluteError(popYes._1, yes._1),
        absoluteError(popYes._2, yes._2))
    })

    import spark.implicits._

    val df = table.toDF()
    // display
    showTable(df)
    val graph = showGraph(spark, df)
    val graphsc = spark.sparkContext.parallelize(Seq(graph))
    graphsc.coalesce(1).saveAsTextFile(filePath("graph.json"))

    spark.stop()
  }

  def estimate(population: RDD[(String, Int)], sampleSize: Double): Unit = {
    def saveToFile(index: Int, items: RDD[(String, (Double, Double, Double))]): Unit = {
      items.coalesce(numPartitions = 1).saveAsObjectFile(filePath(s"bootstrap/estimate_${fmt(sampleSize)}_$index"))
    }

    def fmt(num: Double): String = {
      num.toString.replace(".", "_")
    }

    def resample(popSample: RDD[(String, Int)]): RDD[(String, (Double, Double, Double))] = {
      val newSample = popSample.sample(withReplacement = true, 1)
      newSample.groupByKey().map(x => (x._1, (sampleSize, mean(x._2), variance(x._2))))
    }

    val popSample = population.sample(withReplacement = false, sampleSize).cache()
    1.to(1000).foreach(x => saveToFile(x, resample(popSample)))
  }

  def variance(in: Iterable[Int]): Double = {
    val _mean = mean(in)
    in.map(x => sqr(x - _mean)).sum / (in.count(_ => true) - 1)
  }

  def sqr(x: Double): Double = x * x

  def mean(in: Iterable[Int]): Double = {
    in.sum / in.count(_ => true)
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
      .encodeX("spark", Nom, sortField = Sort("users count", AggOps.Mean))
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
}
