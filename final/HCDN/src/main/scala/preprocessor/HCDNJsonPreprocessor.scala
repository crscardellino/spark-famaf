package preprocessor

import java.io.File
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object HCDNJsonPreprocessor {
  private case class Params(
      input: String = "",
      output: String = "")

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Params]("preprocessor") {
      head("preprocessor", "1.0")

      opt[String]('i', "input").required().action((input, config) =>
        config.copy(input = input)
      ).validate { input =>
        val datasetFile: File = new File(input)
        if (datasetFile.exists()) success
        else failure("The provided dataset path must be a valid file or directory")
      }.text("Path to the dataset json file")

      opt[String]('o', "output").required().action((output, config) =>
        config.copy(output = output)
      ).validate { output =>
        val outputDirectory: File = new File(output)
        if (outputDirectory.exists && outputDirectory.isDirectory) success
        else failure("The provided output path must be a valid directory")
      }.text("Path to the output directory to store the parquet files")
    }

    parser.parse(args, Params()) match {
      case Some(params) =>
        run(params)
      case None =>
        sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val datasetFilePath: String = new File(params.input).getAbsolutePath
    val outputDirPath: String = new File(params.output).getAbsolutePath

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("HCDN Json Preprocessor App")
      .getOrCreate()

    import spark.implicits._

    val rawDataFrame: DataFrame = spark.read.json(datasetFilePath)

    rawDataFrame.select($"id", $"date", $"summary", $"law_text")
      .write.format("parquet").save(s"$outputDirPath/laws.parquet")

    rawDataFrame.select($"id", explode($"signers").as("signers"))
      .select($"id", $"signers.congressman".as("congressman"),
        $"signers.district".as("district"), $"signers.party".as("party"))
      .write.format("parquet").save(s"$outputDirPath/signers.parquet")

    rawDataFrame.select($"id", explode($"scommissions").as("commission"))
      .write.format("parquet").save(s"$outputDirPath/senateCommissions.parquet")

    rawDataFrame.select($"id", explode($"dcommissions").as("commission"))
      .write.format("parquet").save(s"$outputDirPath/deputyCommissions.parquet")

    rawDataFrame.select($"id", explode($"results").as("results"))
      .select($"id", $"results.chamber".as("chamber"),
        $"results.result".as("result"), $"results.date".as("date"))
      .write.format("parquet").save(s"$outputDirPath/results.parquet")

    spark.stop()
  }
}

