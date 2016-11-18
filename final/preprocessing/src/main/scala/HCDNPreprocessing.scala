import java.io.File
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

// Config case class (for argument parsing)
case class Config(datasetPath: String = "", outputDatasetDir: String = "")

// Datasets case classes
case class LawsDataset(id: String, date: String, summary: String, law_text: String)

case class LawSignersDataset(id: String, congressman: String, district: String, party: String)

case class SenateCommissionsDataset(id: String, commission: String)

case class DeputyCommissionsDataset(id: String, commission: String)

case class ResultsDataset(id: String, chamber: String, result: String, date: String)


object HCDNPreprocessing {
  def main(args: Array[String]): Unit = {
    // SparkSession configurations
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("HCDN Preprocessing App")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val parser = new scopt.OptionParser[Config]("HCDNPreprocessing") {
      head("HCDNPreprocessing", "1.0")

      opt[String]('d', "dataset").required().action( (datasetPath, config) =>
        config.copy(datasetPath = datasetPath)
      ).validate { datasetPath =>
        val datasetFile: File = new File(datasetPath)
        if (datasetFile.exists()) success
        else failure("The provided dataset path must be a valid file or directory")
      }.text("Path to the dataset directory or file")

      opt[String]('o', "output").required().action( (outputDatasetDir, config) =>
        config.copy(outputDatasetDir = outputDatasetDir)
      ).validate { datasetPath =>
        val outputPath: File = new File(datasetPath)
        if (outputPath.exists && outputPath.isDirectory) success
        else failure("The provided output path must be a valid directory")
      }.text("Path to the output directory")
    }

    var datasetFile: String = null
    var outputDir: String = null

    parser.parse(args, Config()) match {
      case Some(config) =>
        datasetFile = new File(config.datasetPath).getAbsolutePath
        outputDir = new File(config.outputDatasetDir).getAbsolutePath
      case None =>
        spark.stop()
        sys.exit(1)
    }

    val rawDataFrame: DataFrame = spark.read.json(datasetFile)

    // Saving datasets
    rawDataFrame.select($"id", $"date", $"summary", $"law_text")
      .as[LawsDataset].write.format("parquet").save(s"$outputDir/laws.parquet")

    rawDataFrame.select($"id", explode($"signers").as("signers"))
      .select($"id", $"signers.congressman".as("congressman"),
        $"signers.district".as("district"), $"signers.party".as("party"))
      .as[LawSignersDataset].write.format("parquet").save(s"$outputDir/signers.parquet")

    rawDataFrame.select($"id", explode($"scommissions").as("commission"))
      .as[SenateCommissionsDataset].write.format("parquet").save(s"$outputDir/senateCommissions.parquet")

    rawDataFrame.select($"id", explode($"dcommissions").as("commission"))
      .as[DeputyCommissionsDataset].write.format("parquet").save(s"$outputDir/deputyCommissions.parquet")

    rawDataFrame.select($"id", explode($"results").as("results"))
      .select($"id", $"results.chamber".as("chamber"),
        $"results.result".as("result"), $"results.date".as("date"))
      .as[ResultsDataset].write.format("parquet").save(s"$outputDir/results.parquet")

    spark.stop()
  }
}
