package postprocessor

import java.io.File
import org.apache.spark.sql.{DataFrame, SparkSession}
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
        params.copy(inputLDATopics = inputTransformed)
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

    val laws = spark.read.parquet(s"$inputDatasetDir/laws.parquet")
    val results = spark.read.parquet(s"$inputDatasetDir/results.parquet")
    val signers = spark.read.parquet(s"$inputDatasetDir/signers.parquet")
    val deputyCommissions = spark.read.parquet(s"$inputDatasetDir/deputyCommissions.parquet")
    val senateCommissions = spark.read.parquet(s"$inputDatasetDir/senateCommissions.parquet")

    val LDATopics: DataFrame = spark.read.parquet(s"$inputTopicsPath")
    val LDATransformations: DataFrame = spark.read.parquet(s"$inputTransformedPath")

    // Topic with the most projects
    val topTopics: DataFrame = LDATransformations.orderBy($"id", desc("topicDistribution"))
      .groupBy($"id").agg(first($"pos").as("maxTopic"), max($"topicDistribution"))
      .groupBy($"maxTopic").count.orderBy(desc("count")).limit(5)

    LDATopics.join(topTopics, LDATopics("topic") === topTopics("maxTopic"))
      .map(row => (row.getAs[Seq[String]]("terms").mkString(" "), row.getAs[Long]("count")))
      .coalesce(1).write.format("csv").save(s"$outputDirPath/topTopics.csv")

    // Topic with the most sanctioned projects
    val sanctionedProjects: DataFrame = results.filter($"results" === "SANCIONADO").select($"id")

    val topSanctionedTopics: DataFrame = LDATransformations.join(sanctionedProjects, "id")
      .orderBy($"id", desc("topicDistribution")).groupBy($"id")
      .agg(max($"topicDistribution"), first($"pos").as("maxTopic"))
      .groupBy($"maxTopic").count().orderBy(desc("count")).limit(5)

    LDATopics.join(topSanctionedTopics, LDATopics("topic") === topSanctionedTopics("maxTopic"))
      .map(row => (row.getAs[Seq[String]]("terms").mkString(" "), row.getAs[Long]("count")))
      .coalesce(1).write.format("csv").save(s"$outputDirPath/topSanctionedTopics.csv")

    // Topic with the most projects with half sanctions
    val halfSanctionedProjects: DataFrame = results.filter($"results" === "MEDIA SANCION").select($"id")

    val topHalfSanctionedTopics: DataFrame = LDATransformations.join(halfSanctionedProjects, "id")
      .orderBy($"id", desc("topicDistribution")) .groupBy($"id")
      .agg(max($"topicDistribution"), first($"pos").as("maxTopic"))
      .groupBy($"maxTopic").count().orderBy(desc("count")).limit(5)

    LDATopics.join(topHalfSanctionedTopics, LDATopics("topic") === topHalfSanctionedTopics("maxTopic"))
      .map(row => (row.getAs[Seq[String]]("terms").mkString(" "), row.getAs[Long]("count")))
      .coalesce(1).write.format("csv").save(s"$outputDirPath/topHalfSanctionedTopics.csv")

    // Topic with the most retired projects
    val retiredProjects: DataFrame = results.filter($"results" === "RETIRADO").select($"id")

    val topRetiredTopics: DataFrame = LDATransformations.join(retiredProjects, "id")
      .orderBy($"id", desc("topicDistribution")) .groupBy($"id")
      .agg(max($"topicDistribution"), first($"pos").as("maxTopic"))
      .groupBy($"maxTopic").count().orderBy(desc("count")).limit(5)

    LDATopics.join(topRetiredTopics, LDATopics("topic") === topRetiredTopics("maxTopic"))
      .map(row => (row.getAs[Seq[String]]("terms").mkString(" "), row.getAs[Long]("count")))
      .coalesce(1).write.format("csv").save(s"$outputDirPath/topRetiredTopics.csv")

    // Top topic in the top 10 parties
    val topParties: Set[String] = signers.groupBy($"party")
      .agg(countDistinct($"id").as("count"))
      .orderBy(desc("count")).limit(10).select($"party")
      .map(row => row.getString(0)).collectAsList.toSet

    val topTopicsForTopParties: DataFrame = LDATransformations.join(signers, "id")
      .select($"id", $"party", $"pos".as("topic"), $"topicDistribution")
      .filter(row => topParties contains row.getAs[String]("party")).dropDuplicates("id", "topic")
      .where($"topicDistribution" >= params.minimumDistribution)
      .agg(avg($"topicDistribution").as("topicDistributionAvg"))
      .orderBy($"party", desc("topicDistributionAvg")).groupBy($"party")
      .agg(first($"topic").as("maxTopic"), max($"topicDistributionAvg").as("topicDistributionAvgMax"))
      .orderBy($"party")

    LDATopics.join(topTopicsForTopParties, LDATopics("topic") === topTopicsForTopParties("maxTopic"))
      .map(row =>
        (row.getAs[String]("party"), row.getAs[Int]("topic"), row.getAs[Double]("topicDistributionAvgMax"),
         row.getAs[Seq[String]]("terms").mkString(" ")))
      .coalesce(1).write.format("csv").save(s"$outputDirPath/topTopicsForTopParties.csv")


    // Top topic in the top 10 districts
    val topDistricts: Set[String] = signers.groupBy($"district")
      .agg(countDistinct($"id").as("count"))
      .orderBy(desc("count")).limit(10).select($"district")
      .map(row => row.getString(0)).collectAsList.toSet

    val topTopicsForTopDistricts: DataFrame = LDATransformations.join(signers, "id")
      .select($"id", $"district", $"pos".as("topic"), $"topicDistribution")
      .filter(row => topDistricts contains row.getAs[String]("district")).dropDuplicates("id", "topic")
      .where($"topicDistribution" >= params.minimumDistribution)
      .agg(avg($"topicDistribution").as("topicDistributionAvg"))
      .orderBy($"district", desc("topicDistributionAvg")).groupBy($"district")
      .agg(first($"topic").as("maxTopic"), max($"topicDistributionAvg").as("topicDistributionAvgMax"))
      .orderBy($"district")

    LDATopics.join(topTopicsForTopDistricts, LDATopics("topic") === topTopicsForTopDistricts("maxTopic"))
      .map(row =>
        (row.getAs[String]("district"), row.getAs[Int]("topic"), row.getAs[Double]("topicDistributionAvgMax"),
          row.getAs[Seq[String]]("terms").mkString(" ")))
      .coalesce(1).write.format("csv").save(s"$outputDirPath/topTopicsForTopDistricts.csv")

    // Top topic in the top 10 congressmen
    val topCongressmen: Set[String] = signers.groupBy($"congressman")
      .agg(countDistinct($"id").as("count"))
      .orderBy(desc("count")).limit(10).select($"congressman")
      .map(row => row.getString(0)).collectAsList.toSet

    val topTopicsForTopCongressmen: DataFrame = LDATransformations.join(signers, "id")
      .select($"id", $"congressman", $"pos".as("topic"), $"topicDistribution")
      .filter(row => topCongressmen contains row.getAs[String]("congressman")).dropDuplicates("id", "topic")
      .where($"topicDistribution" >= params.minimumDistribution)
      .agg(avg($"topicDistribution").as("topicDistributionAvg"))
      .orderBy($"congressman", desc("topicDistributionAvg")).groupBy($"congressman")
      .agg(first($"topic").as("maxTopic"), max($"topicDistributionAvg").as("topicDistributionAvgMax"))
      .orderBy($"congressman")

    LDATopics.join(topTopicsForTopCongressmen, LDATopics("topic") === topTopicsForTopCongressmen("maxTopic"))
      .map(row =>
        (row.getAs[String]("congressman"), row.getAs[Int]("topic"), row.getAs[Double]("topicDistributionAvgMax"),
          row.getAs[Seq[String]]("terms").mkString(" ")))
      .coalesce(1).write.format("csv").save(s"$outputDirPath/topTopicsForTopCongressmen.csv")

    // Top topic in the top 10 deputy commissions

    val topDeputyCommissions: Set[String] = deputyCommissions.groupBy($"commission")
      .agg(countDistinct($"id").as("count"))
      .orderBy(desc("count")).limit(10).select($"commission")
      .map(row => row.getString(0)).collectAsList.toSet

    val topTopicsForTopDeputyCommissions: DataFrame = LDATransformations.join(deputyCommissions, "id")
      .select($"id", $"commission", $"pos".as("topic"), $"topicDistribution")
      .filter(row => topDeputyCommissions contains row.getAs[String]("commission")).dropDuplicates("id", "topic")
      .where($"topicDistribution" >= params.minimumDistribution)
      .agg(avg($"topicDistribution").as("topicDistributionAvg"))
      .orderBy($"commission", desc("topicDistributionAvg")).groupBy($"commission")
      .agg(first($"topic").as("maxTopic"), max($"topicDistributionAvg").as("topicDistributionAvgMax"))
      .orderBy($"commission")

    LDATopics.join(
      topTopicsForTopDeputyCommissions, LDATopics("topic") === topTopicsForTopDeputyCommissions("maxTopic")
    ).map(row =>
        (row.getAs[String]("commission"), row.getAs[Int]("topic"), row.getAs[Double]("topicDistributionAvgMax"),
          row.getAs[Seq[String]]("terms").mkString(" ")))
      .coalesce(1).write.format("csv").save(s"$outputDirPath/topTopicsForTopDeputyCommission.csv")

    // Top topic in the top 10 senate commissions

    val topSenateCommissions: Set[String] = senateCommissions.groupBy($"commission")
      .agg(countDistinct($"id").as("count"))
      .orderBy(desc("count")).limit(10).select($"commission")
      .map(row => row.getString(0)).collectAsList.toSet

    val topTopicsForTopSenateCommissions: DataFrame = LDATransformations.join(senateCommissions, "id")
      .select($"id", $"commission", $"pos".as("topic"), $"topicDistribution")
      .filter(row => topSenateCommissions contains row.getAs[String]("commission")).dropDuplicates("id", "topic")
      .where($"topicDistribution" >= params.minimumDistribution)
      .agg(avg($"topicDistribution").as("topicDistributionAvg"))
      .orderBy($"commission", desc("topicDistributionAvg")).groupBy($"commission")
      .agg(first($"topic").as("maxTopic"), max($"topicDistributionAvg").as("topicDistributionAvgMax"))
      .orderBy($"commission")

    LDATopics.join(
      topTopicsForTopSenateCommissions, LDATopics("topic") === topTopicsForTopSenateCommissions("maxTopic")
    ).map(row =>
      (row.getAs[String]("commission"), row.getAs[Int]("topic"), row.getAs[Double]("topicDistributionAvgMax"),
        row.getAs[Seq[String]]("terms").mkString(" ")))
      .coalesce(1).write.format("csv").save(s"$outputDirPath/topTopicsForTopSenateCommission.csv")
  }
}

