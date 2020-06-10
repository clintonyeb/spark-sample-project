//package samples
//import Task
//
//object Demo extends Task {
//  override def run(args: Array[String]): Unit = {
//    val spark = session("Demo App")
//    val logFile = "/usr/local/spark/README.md" // Should be some file on your system
//    val logData = spark.read.textFile(logFile).cache()
//    val numAs = logData.filter(line => line.contains("a")).count()
//    val numBs = logData.filter(line => line.contains("b")).count()
//    println(s"Lines with a: $numAs, Lines with b: $numBs")
//    spark.stop()
//  }
//}
