import org.apache.spark.sql.SparkSession

trait Task {
  def run(args: Array[String]): Unit
  val HOST_NAME = "master"
  val HDFS_PORT = "7077"

  def session(appName: String): SparkSession = {
    SparkSession.builder.appName(appName).getOrCreate()
  }

  def filePath(file: String): String = {
    s"file:///tmp/data/$file"
  }
}
