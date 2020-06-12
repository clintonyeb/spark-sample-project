import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


case class PopulationTable(Percentage: Double, UnEmployedMean: Double, UnEmployedVariance: Double, EmployedMean: Double, EmployedVariance: Double)

object Main {

  def sqr(x: Double): Double = x * x

  def mean[T](ts: Iterable[T] )( implicit num: Numeric[T] ): Double = {
    num.toDouble( ts.sum ) / ts.size
  }

  def variance[T](ts: Iterable[T] )( implicit num: Numeric[T] ): Double = {
    val m = mean(ts)
    ts.map(x => sqr(num.toDouble(x) - m)).sum / (ts.size - 1)
  }

  def filePath(file: String): String = {
//    val HOST_NAME = "master"
//    val HDFS_PORT = "7077"
    s"file:///tmp/data/$file"
  }

  def main(args: Array[String]): Unit = {
    println("Application Started")

    val spark = SparkSession.builder.appName("Bootstrap App").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    def showCategoriesComputation(data: RDD[(Boolean, (Double, Double))]): Unit = {
      val df = data.map(x => (if (x._1) "Yes" else "No", x._2._1, x._2._2)).toDF(colNames = "Employed", "Mean Age", "Variance Age")
      df.show()
    }

    def absoluteError(actual: Double, estimate: Double): Double = {
      (actual - estimate).abs * 100 / actual
    }

    // saved as [(key, sampleSize), (mean, variance)]
    def retrieveEstimates(): RDD[(Boolean, (Double, Double, Double))] = {
      spark.sparkContext.objectFile(filePath("bootstrap/estimate*/*"))
    }

    def showGraph(df: sql.DataFrame): String = {
      import vegas._
      import vegas.sparkExt._

      val plot = Vegas("Spark")
        .withDataFrame(df)
        .mark(Bar) // Change to .mark(Area)
        .encodeX("spark", Nom, sortField = Sort("users count", AggOps.Mean))
        .encodeY("users_count", Quant)

//      def renderHTML(): Unit = {
//        plot.html.pageHTML() + "\n" + // a complete HTML page containing the plot
//          plot.html.frameHTML("plot") // an iframe containing the plot
//      }
//
//      def renderWindow(): Unit = {
//        plot.window.show
//      }

      plot.toJson
    }

    println("Reading CSV file")
    val csv = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath("arrests.csv"))
    val data = csv.select("employed", "age")
    data.printSchema()
    data.show()

    // employment("Yes"/"NO") and age(Int)
    // No: false, Yes: true
    println("Computing Original Metrics")
    val population: RDD[(Boolean, Int)] = data.map(p => if (p.getString(0) == "Yes") (true, p.getInt(1)) else (false, p.getInt(1))).rdd.cache() // broadcast this variable
    val popGroup: RDD[(Boolean, (Double, Double))] = population.groupByKey().map(x => (x._1, (mean(x._2), variance(x._2)))).cache()

    // Display for original
    showCategoriesComputation(popGroup)

    val popNo: (Double, Double) = popGroup.filter(x => !x._1).first()._2
    val popYes: (Double, Double) = popGroup.filter(x => x._1).first()._2

    println("Starting computations for estimates")
    val percentages = List(.05, .15, .25, .35, .45, .55, .65, .75, .85, .95)
    //    val percentages = List(.25)
    percentages.par.foreach(p => (p, estimate(population, p, spark)))

    println("Estimates done. Retrieving from filesystem")
    val estimates: RDD[(Boolean, (Double, Double, Double))] = retrieveEstimates()
    val estimatesGrouped: RDD[((Boolean, Double), (Double, Double))] = estimates
      .map(x => ((x._1, x._2._1), (x._2._2, x._2._3)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => ((x._1._1, x._1._2), (x._2._1 / 1000, x._2._2 / 1000)))

    val estimatesGroupedBySampleSize: RDD[(Double, Iterable[(Boolean, (Double, Double))])] =
      estimatesGrouped.map(x => (x._1._2, (x._1._1, (x._2._1, x._2._2)))).groupByKey()

    println("Computing Error Rates")
    val table = estimatesGroupedBySampleSize.map(estimate => {
      val sampleSize: Double = estimate._1
      val value: Iterable[(Boolean, (Double, Double))] = estimate._2
      val map: Map[Boolean, (Double, Double)] = value.groupBy(x => x._1).mapValues(x => x.head._2)
      val no = map(false)
      val yes = map(true)
      PopulationTable(sampleSize,
        absoluteError(popNo._1, no._1),
        absoluteError(popNo._2, no._2),
        absoluteError(popYes._1, yes._1),
        absoluteError(popYes._2, yes._2))
    })

    println("Show Table For Error Rates")
    val df = table.toDF()
    df.show()

    println("Show Graph For Error Rates")
    val graph = showGraph(df)
    val graphsc = spark.sparkContext.parallelize(Seq(graph))
    graphsc.coalesce(1).saveAsTextFile(filePath("graph.json"))

    println("Done, stopping Spark")
    spark.stop()
  }

  def estimate(population: RDD[(Boolean, Int)], sampleSize: Double, spark: SparkSession): Unit = {
    import spark.implicits._
    println(s"Computing estimate for: $sampleSize")

    def saveToFile(index: Int, items: RDD[(Boolean, (Double, Double, Double))]): Unit = {
      items.coalesce(numPartitions = 1).saveAsObjectFile(filePath(s"bootstrap/estimate_${fmt(sampleSize)}_$index"))
    }

    def fmt(num: Double): String = {
      num.toString.replace(".", "_")
    }

    def resample(popSample: RDD[(Boolean, Int)]): RDD[(Boolean, (Double, Double, Double))] = {
      val newSample = popSample.sample(withReplacement = true, 1)
      newSample.groupByKey().map(x => (x._1, (sampleSize, mean(x._2), variance(x._2))))
    }

    def showCategoriesComputation(data: RDD[(Boolean, (Double, Double, Double))]): Unit = {
      val df = data.map(x => (if (x._1) "Yes" else "No", x._2._2, x._2._3)).toDF("Employed", "Mean Age", "Variance Age")
      df.show()
    }

    val popSample = population.sample(withReplacement = false, sampleSize).cache() // convert to broadcast variable

    1.to(1000).par.foreach(x => {
      println(s"Computing estimate for sampleSize: $sampleSize, Iteration: $x")
      val estimate: RDD[(Boolean, (Double, Double, Double))] = resample(popSample)
      println(s"Results for sampleSize: $sampleSize, Iteration: $x")
      showCategoriesComputation(estimate)
      saveToFile(x, estimate)
    })

    println(s"Computations done for sampleSize: $sampleSize")
  }
}
