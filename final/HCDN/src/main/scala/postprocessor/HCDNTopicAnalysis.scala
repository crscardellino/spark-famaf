package postprocessor

import java.io.File
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._


object HCDNTopicAnalysis {
  private case class Params(
    inputDatasets: String = "",
    inputLDATopics: String = "",
    inputLDATransformed: String = "",
    output: String = "",
    minimumDistribution: Double = 0.1
  )

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Params]("topicanalysis") {
      head("topicanalysis", "1.0")

      opt[String]('i', "input").required().action((inputTopics, params) =>
        params.copy(inputDatasets = inputTopics)
      ).validate { input =>
        val inputDatasetDir: File = new File(input)
        if (inputDatasetDir.exists() && inputDatasetDir.isDirectory) success
        else failure("The provided dataset path must be a valid directory")
      }.text("Path to the directory with all the datasets for laws, results, signers and commissions.")

      opt[String]('t', "inputtopics").required().action((inputTopics, params) =>
        params.copy(inputLDATopics = inputTopics)
      ).validate { input =>
        val inputFile: File = new File(input)
        if (inputFile.exists()) success
        else failure("The provided dataset path must a valid parquet file")
      }.text("Path to the input topics file")

      opt[String]('l', "inputtransformed").required().action((inputTransformed, params) =>
        params.copy(inputLDATransformed = inputTransformed)
      ).validate { input =>
        val inputFile: File = new File(input)
        if (inputFile.exists()) success
        else failure("The provided dataset path must a valid parquet file")
      }.text("Path to the input laws transformed file")

      opt[String]('o', "output").required().action((output, params) =>
        params.copy(output = output)
      ).validate { output =>
        val outputDirectory: File = new File(output)
        if (outputDirectory.exists && outputDirectory.isDirectory) success
        else failure("The provided output path must be a valid directory")
      }.text("Path to the output directory to store the parquet files")

      opt[Double]('m', "minimumdistribution").action((minimumDistribution, params) =>
        params.copy(minimumDistribution = minimumDistribution)
      )
    }

    parser.parse(args, Params()) match {
      case Some(params) =>
        run(params)
      case None =>
        sys.exit(1)
    }
  }

  private def saveTopTopics(transformationsDataFrame: DataFrame, LDATopics: DataFrame,
                           spark: SparkSession, savePath: String): Unit = {
    import spark.implicits._

    val topTopics: DataFrame = transformationsDataFrame.orderBy($"id", desc("topicDistribution"))
      .groupBy($"id")
        .agg(first($"pos").as("maxTopic"), max($"topicDistribution"))
      .groupBy($"maxTopic")
        .count
        .orderBy(desc("count"))
        .limit(5)

    LDATopics.join(topTopics, LDATopics("topic") === topTopics("maxTopic"))
      .map(row => (row.getAs[Seq[String]]("terms").mkString(" "), row.getAs[Long]("count")))
      .coalesce(1).write.format("csv").save(savePath)
  }

  private def saveTopTopicsForTopColumn(dataset: DataFrame, groupColumn: Column,
                                        LDATopics: DataFrame, LDATransformations: DataFrame,
                                        spark: SparkSession, minimumDistribution: Double,
                                        savePath: String): Unit = {
    import spark.implicits._

    val topInColumn: Set[String] = dataset
      .groupBy(groupColumn)
        .agg(countDistinct($"id").as("count"))
        .orderBy(desc("count"))
        .limit(10)
      .select(groupColumn)
        .map(row => row.getString(0)).collectAsList.toSet

    val topTopicsForTopParties: DataFrame = LDATransformations.join(dataset, "id")
      .select($"id", groupColumn, $"pos".as("topic"), $"topicDistribution")
        .filter(row => topInColumn contains row.getAs[String](groupColumn.toString))
        .where($"topicDistribution" >= minimumDistribution)
        .dropDuplicates("id", "topic")
      .groupBy(groupColumn, $"topic")
        .agg(avg($"topicDistribution").as("topicDistributionAvg"))
        .orderBy(groupColumn, desc("topicDistributionAvg"))
      .groupBy(groupColumn)
        .agg(first($"topic").as("maxTopic"), max($"topicDistributionAvg").as("topicDistributionAvgMax"))
        .orderBy(groupColumn)

    LDATopics.join(topTopicsForTopParties, LDATopics("topic") === topTopicsForTopParties("maxTopic"))
      .map(row => (
        row.getAs[String](groupColumn.toString),
        row.getAs[Int]("topic"),
        row.getAs[Double]("topicDistributionAvgMax"),
        row.getAs[Seq[String]]("terms").mkString(" ")
      ))
      .coalesce(1).write.format("csv").save(savePath)
  }

  def run(params: Params): Unit = {
    val inputDatasetDir: String = new File(params.inputDatasets).getAbsolutePath
    val inputTopicsPath: String = new File(params.inputLDATopics).getAbsolutePath
    val inputTransformedPath: String = new File(params.inputLDATransformed).getAbsolutePath
    val outputDirPath: String = new File(params.output).getAbsolutePath

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("HCDN Topic Analysis App")
      .getOrCreate()

    import spark.implicits._

    val results = spark.read.parquet(s"$inputDatasetDir/results.parquet")
    val signers = spark.read.parquet(s"$inputDatasetDir/signers.parquet")
    val deputyCommissions = spark.read.parquet(s"$inputDatasetDir/deputyCommissions.parquet")
    val senateCommissions = spark.read.parquet(s"$inputDatasetDir/senateCommissions.parquet")

    val LDATopics: DataFrame = spark.read.parquet(s"$inputTopicsPath")
    val LDATransformations: DataFrame = spark.read.parquet(s"$inputTransformedPath")

    // Topic with the most projects
    saveTopTopics(LDATransformations, LDATopics, spark, s"$outputDirPath/topTopics.csv")

    // Topic with the most sanctioned projects
    val sanctionedProjects: DataFrame = results.filter($"results" === "SANCIONADO").select($"id")
    saveTopTopics(LDATransformations.join(sanctionedProjects, "id"),
      LDATopics, spark, s"$outputDirPath/topSanctionedTopics.csv")

    // Topic with the most projects with half sanctions
    val halfSanctionedProjects: DataFrame = results.filter($"results" === "MEDIA SANCION").select($"id")
    saveTopTopics(LDATransformations.join(halfSanctionedProjects, "id"),
      LDATopics, spark, s"$outputDirPath/topHalfSanctionedTopics.csv")

    // Topic with the most retired projects
    val retiredProjects: DataFrame = results.filter($"results" === "RETIRADO").select($"id")
    saveTopTopics(LDATransformations.join(retiredProjects, "id"),
      LDATopics, spark, s"$outputDirPath/topRetiredTopics.csv")

    // Top topic in the top 10 parties
    saveTopTopicsForTopColumn(signers, $"party", LDATopics, LDATransformations, spark, params.minimumDistribution,
      s"$outputDirPath/topTopicsForTopParties.csv")

    // Top topic in the top 10 districts
    saveTopTopicsForTopColumn(signers, $"district", LDATopics, LDATransformations, spark, params.minimumDistribution,
      s"$outputDirPath/topTopicsForTopDistricts.csv")

    // Top topic in the top 10 congressmen
    saveTopTopicsForTopColumn(signers, $"congressman", LDATopics, LDATransformations, spark, params.minimumDistribution,
      s"$outputDirPath/topTopicsForTopCongressmen.csv")

    // Top topic in the top 10 deputy commissions
    saveTopTopicsForTopColumn(deputyCommissions, $"commission", LDATopics, LDATransformations, spark,
      params.minimumDistribution, s"$outputDirPath/topTopicsForTopDeputyCommissions.csv")

    // Top topic in the top 10 senate commissions
    saveTopTopicsForTopColumn(senateCommissions, $"commission", LDATopics, LDATransformations, spark,
      params.minimumDistribution, s"$outputDirPath/topTopicsForTopSenateCommissions.csv")
  }
}

