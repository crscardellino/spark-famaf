import java.io.File
import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.util.Try

// Config case class (for argument parsing)
case class Config(datasetDirOrFile: String = "", sessionDuration: Long = -1)

// Hits Dataset case class (for reading data log files)
case class HitsDataset(timestamp: Timestamp, userid: String, event: String, amount: Double)

// Sessions Dataset case class
case class SessionsDataset(userid: String, start: Timestamp, end: Timestamp,
                           renders: Int, plays: Int, checkouts: Int, amount: Double)

// Enumeration for events ids
object Event extends Enumeration {
  val Render, Play, Checkout = Value
}


object SessionizationJob {
  val eventsIds = Map(
    Event.Render -> "12c6fc06c99a462375eeb3f43dfd832b08ca9e17",
    Event.Play -> "bd307a3ec329e10a2cff8fb87480823da114f8f4",
    Event.Checkout -> "7b52009b64fd0a2a49e6d8a939753077792b0554"
  )

  def main(args: Array[String]) {
    // SparkSession configurations
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val parser = new scopt.OptionParser[Config]("SessionizationJob") {
      head("SessionizationJob", "1.0")

      opt[String]('d', "dataset").required().action( (dirOrFile, config) =>
        config.copy(datasetDirOrFile = dirOrFile)
      ).validate { dirOrFile =>
        val dof: File = new File(dirOrFile)
        if (dof.exists()) success
        else failure("The provided dataset path must be a valid file or directory")
      }.text("Path to the dataset directory or file")

      opt[Long]('s', "session").required().action( (session, config) =>
        config.copy(sessionDuration = session)
      ).validate( session =>
        if (session > 0) success
        else failure("Session must be a positive integer")
      ).text("Session duration (in minutes)")
    }

    var datasetDirOrFile: File = null
    var sessionDuration: Long = 0L

    parser.parse(args, Config()) match {
      case Some(config) =>
        datasetDirOrFile = new File(config.datasetDirOrFile)
        sessionDuration = config.sessionDuration
      case None =>
        spark.stop()
        sys.exit(1)
    }

    val rawDataset: Dataset[String] =
      if (datasetDirOrFile.isDirectory)
        spark.read.text(datasetDirOrFile.listFiles.filter(_.isFile).map(_.getAbsolutePath): _*).as[String]
      else
        spark.read.text(datasetDirOrFile.getAbsolutePath).as[String]

    val hitsDataset: Dataset[HitsDataset] = rawDataset.filter(_.trim.split("\t").length == 4)
      .map(s => s.split("\t")).map(r =>
      HitsDataset(Timestamp.valueOf(r(0)), r(1), r(2), Try(r(3).toDouble).getOrElse(0.0)))

    // Window functions
    val window = Window.partitionBy('userid).orderBy('timestamp)
    val tsDiff = lag('epoch, 1).over(window)
    val sessionSum = sum('newSession).over(window)

    def toEpoch: Timestamp => Long = _.getTime / 60000L

    val sessionizedDataFrame: DataFrame = hitsDataset.withColumn("epoch", udf(toEpoch).apply('timestamp))
      .withColumn("diff", 'epoch - tsDiff).withColumn("newsession", ('diff > sessionDuration).cast("integer"))
      .drop("epoch", "diff").na.fill(0, Seq("newsession")).withColumn("sessionid", sessionSum)

    val sessionsDataset: Dataset[SessionsDataset] = sessionizedDataFrame.groupBy('userid, 'sessionid)
      .agg(first('timestamp).as("start"), last('timestamp).as("end"),
        sum(when('event === eventsIds(Event.Render), 1).otherwise(0)).cast("integer").as("renders"),
        sum(when('event === eventsIds(Event.Play), 1).otherwise(0)).cast("integer").as("plays"),
        sum(when('event === eventsIds(Event.Checkout), 1).otherwise(0)).cast("integer").as("checkouts"),
        sum('amount).as("amount")).drop('sessionid).as[SessionsDataset]

    // Save the hits dataset
    hitsDataset.write.format("parquet").save("hitsDataset.parquet")

    // Save sessions dataset
    sessionsDataset.write.format("parquet").save("sessionsDataset.parquet")

    spark.stop()
  }
}
