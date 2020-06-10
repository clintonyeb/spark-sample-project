import org.apache.spark.rdd.RDD

import scala.collection.mutable

case class PopulationTable(population: Double,noMean: Double, noVariance: Double, yesMean: Double, yesVariance: Double)

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
    val popGroup = population.groupByKey().map(x => (x._1, (mean(x._2), variance(x._2)))).sortByKey()
    println("Original:")
    val originalResults = popGroup.collect()
    print(originalResults)

    def absoluteError(actual: Double, estimate: Double): Double = {
      ((actual - estimate).abs * 100) / actual
    }

    import spark.sqlContext.implicits._

//    val computations = ListBuffer[collection.Map[String, (Double, Double)]]()
    val percentages = List(5, 15, 25, 35, 45, 55, 65, 75, 85, 95)
    val perc = spark.sparkContext.parallelize(percentages)
    val df = perc.map(p => (p, estimate(population, p)))
      .map(e => PopulationTable(e._1,
        absoluteError(originalResults(0)._2._1, e._2("No")._1),
        absoluteError(originalResults(0)._2._2, e._2("No")._2),
        absoluteError(originalResults(1)._2._1, e._2("Yes")._1),
        absoluteError(originalResults(1)._2._2, e._2("Yes")._2)))
      .toDF()
    df.show()

    spark.stop()
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

    println("Results:")
    println(avg)
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
