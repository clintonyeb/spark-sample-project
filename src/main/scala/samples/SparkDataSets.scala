//package samples
//import _root_.Task
//
///** ************************************************
// * Note: In Maven dependency                      *
// * Spark and Spark SQL must have the same version *
// * This code is created using 2.11 version 2.3.0  *
// * *
// * The env. variable                              *
// * *
// * SPARK_LOCAL_HOSTNAME=localhost                 *
// * *
// * must be set in Windows 10                      *
// * **************************************************/
//
////import org.apache.spark.sql.{DataFrame, SQLContext}
//
////case class Person(name: String, age: Int)
////
////case class Cars(car: String, mpg: String, cyl: String, disp: String, hp: String,
////                drat: String, wt: String, qsec: String, vs: String, am: String, gear: String, carb: String)
//
//
//object SparkDataSets extends Task {
//  override def run(args: Array[String]) {
//
//    val spark = session("Spark DataSets")
//    spark.sparkContext.setLogLevel("WARN")
//
//    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
//    import sqlContext.implicits._
//
//    val csv = spark.sparkContext.textFile(filePath("mtcars.csv"))
//
//    // Create a Spark DataFrame
//    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
//    val header = headerAndRows.first
//    val mtcdata = headerAndRows.filter(_ (0) != header(0))
//    val mtcars = mtcdata
//      .map(p => Cars(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11)))
//      .toDF
//    mtcars.printSchema
//    mtcars.select("mpg").show(5)
//    mtcars.filter(mtcars("mpg") < 18).show()
//
//    // Aggregate data after grouping by columns
//    import org.apache.spark.sql.functions._
//    mtcars.groupBy("cyl").agg(avg("wt")).show()
//    mtcars.groupBy("cyl").agg(count("wt")).sort($"count(wt)".desc).show()
//
//    // Operate on columns
//    mtcars.withColumn("wtTon", mtcars("wt") * 0.45).select("car", "wt", "wtTon").show(6)
//    // Run SQL queries from the Spark DataSet
//
//
//    val mtcarsDS = mtcdata
//      .map(p => Cars(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11)))
//      .toDS
//
//    mtcarsDS.printSchema
//    mtcarsDS.select("mpg").show(5)
//    mtcarsDS.filter($"mpg" < 18).show()
//    // Aggregate data after grouping by columns
//    mtcarsDS.groupBy("cyl").agg(avg("wt")).show()
//    mtcarsDS.groupBy("cyl").agg(count("wt")).sort($"count(wt)".desc).show()
//    // Operate on columns
//    mtcarsDS.select($"wt" * 0.45 as "wt45", $"cyl" as "Cylinder").show(6)
//
//
//    // Datasets from Scala objects
//    val info = List(("mike", 24), ("joe", 34), ("jack", 55))
//    val infoRDD = spark.sparkContext.parallelize(info)
//    val people = infoRDD.map(r => Person(r._1, r._2)).toDS()
//    people.show()
//
//    val peopleAsTuple = infoRDD.map(r => (r._1, r._2)).toDS()
//    peopleAsTuple.show()
//
//    spark.stop()
//  }
//}
