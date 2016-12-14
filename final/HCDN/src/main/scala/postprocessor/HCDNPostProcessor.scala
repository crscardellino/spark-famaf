package postprocessor

import java.io.File
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._


class HCDNPostProcessor {
  private case class Params(
      input: String = "",
      output: String = "",
      topics: Seq[Int] = Seq()
  )

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Params]("preprocessor") {
      head("preprocessor", "1.0")

      opt[String]('i', "input").required().action((input, params) =>
        params.copy(input = input)
      ).validate { input =>
        val datasetFile: File = new File(input)
        if (datasetFile.exists()) success
        else failure("The provided dataset path must be the valid laws.parquet file")
      }.text("Path to the dataset laws.parquet file")

      opt[String]('o', "output").required().action((output, params) =>
        params.copy(output = output)
      ).validate { output =>
        val outputDirectory: File = new File(output)
        if (outputDirectory.exists && outputDirectory.isDirectory) success
        else failure("The provided output path must be a valid directory")
      }.text("Path to the output directory to store the parquet files")

      opt[Seq[Int]]('t', "topics").valueName("<n1>,<n2>...").required().action(
        (topics, params) => params.copy(topics = topics)
      ).text("Different topics amount to read")
    }

    parser.parse(args, Params()) match {
      case Some(params) =>
        run(params)
      case None =>
        sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val inputDirPath: String = new File(params.input).getAbsolutePath
    val outputDirPath: String = new File(params.output).getAbsolutePath

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("HCDN Topic Modelling App")
      .getOrCreate()

    import spark.implicits._

    val vectorToArray: UserDefinedFunction = udf { vector: Vector => vector.toArray }

    def loadDatasets(topicsList: Seq[Int], optimizer: String, baseDir: String): Map[Int, (DataFrame, DataFrame)] = {
      def loadDatasetsRec(remainingTopics: Seq[Int], currentDatasets: Map[Int, (DataFrame, DataFrame)]):
      Map[Int, (DataFrame, DataFrame)] = {
        remainingTopics match {
          case topic :: rTopics =>
            val ldaTopics: DataFrame = spark.read
              .parquet(s"$baseDir/LDA_${topic}_topics_${optimizer}_optimizer.parquet")
            val ldaTransformations: DataFrame = spark.read
              .parquet(s"$baseDir/laws_LDA_${topic}_topics_${optimizer}_optimizer.parquet")
            loadDatasetsRec(rTopics, currentDatasets + (topic -> (ldaTopics,
              ldaTransformations.select($"id", vectorToArray($"topicDistribution").as("topicDistribution")))))
          case Nil => currentDatasets
        }
      }

      loadDatasetsRec(topicsList, Map())
    }

    val LDADatasets: Map[String, Map[Int, (DataFrame, DataFrame)]] = Map(
      "em" -> loadDatasets(params.topics, "em", inputDirPath),
      "online" -> loadDatasets(params.topics, "online", inputDirPath)
    )

    for { (optimizer, datasets) <- LDADatasets
          (topicCount, (ldaTopics, ldaTransformations)) <- datasets }
    {
      val ldaTerm: DataFrame = ldaTopics.select($"topic", posexplode($"terms"))
        .select($"topic", $"pos", $"col".as("term"))
      val ldaTermWeight: DataFrame = ldaTopics.select($"topic", posexplode($"termWeights"))
        .select($"topic", $"pos", $"col".as("termWeight"))

      val expandedLDATopics: DataFrame = ldaTerm.join(ldaTermWeight,
        (ldaTerm("topic") === ldaTermWeight("topic")) && (ldaTerm("pos") === ldaTermWeight("pos")))
        .select(ldaTerm("topic"), ldaTerm("pos"), $"term", $"termWeight")

      expandedLDATopics.write.format("parquet")
          .save(s"$outputDirPath/LDA_${topicCount}_topics_${optimizer}_optimizer_expanded.parquet")

      val expandedLDATransformations: DataFrame = ldaTransformations
        .select($"id", posexplode($"topicDistribution")).select($"id", $"pos", $"col".as("topicDistribution"))

      expandedLDATransformations.write.format("parquet")
        .save(s"$outputDirPath/laws_LDA_${topicCount}_topics_${optimizer}_optimizer_expanded.parquet")
    }
  }

}
