package clustering

import java.io.File
import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._


object HCDNTopicModelling {
  private case class Params(
      input: String = "",
      output: String = "",
      k: Int = 10,
      iterations: Int = 10,
      optimizer: String = "em")

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

      opt[Int]('k', "topics").required().action((k, params) =>
        params.copy(k = k)
      ).text("Number of topics (clusters)")

      opt[Int]('t', "iterations").action((iter, params) =>
        params.copy(iterations = iter)
      ).text("Maximum number of iterations (defaults to 10)")

      opt[String]('a', "optimizer").action((optimizer, params) =>
        params.copy(optimizer = optimizer)
      ).validate { optimizer =>
        if (optimizer == "em" || optimizer == "online") success
        else failure("The provided algorithm is not valid (must be \"em\" or \"online\"")
      }.text("Optimizer algorithm (defaults to \"em\").")
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
      .appName("HCDN Topic Modelling App")
      .getOrCreate()

    import spark.implicits._

    val lawsDataset: DataFrame = spark.read.parquet(datasetFilePath)
      .withColumn("text", concat($"summary", $"law_text"))

    val tokenizer: RegexTokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("tokens")
      .setPattern("\\p{L}+")
      .setGaps(false)

    val remover: StopWordsRemover = new StopWordsRemover()
      .setStopWords(StopWordsRemover.loadDefaultStopWords("spanish"))
      .setInputCol("tokens")
      .setOutputCol("filteredTokens")

    val tokenizedLawsDataset: DataFrame = remover.transform(tokenizer.transform(lawsDataset))
      .select($"id", $"filteredTokens".as("tokens"))

    val counter: CountVectorizer = new CountVectorizer()
      .setInputCol("tokens")
      .setOutputCol("termFrequency")
      .setMinDF(5)

    val counterModel: CountVectorizerModel = counter.fit(tokenizedLawsDataset)

    val vectorizedLawsDataset: DataFrame = counterModel.transform(tokenizedLawsDataset)

    val lda: LDA = new LDA()
      .setK(params.k)
      .setMaxIter(params.iterations)
      .setOptimizer(params.optimizer)
      .setFeaturesCol("termFrequency")

    val model: LDAModel = lda.fit(vectorizedLawsDataset)

    val mapVocabArray: UserDefinedFunction = udf { indices: Seq[Int] =>
      indices collect counterModel.vocabulary
    }

    val topics: DataFrame = model.describeTopics(10)
      .select($"topic", mapVocabArray($"termIndices").as("terms"), $"termWeights")

    topics.write.format("parquet")
      .save(s"$outputDirPath/LDA_${params.k}_topics_${params.optimizer}_optimizer.parquet")

    model.transform(vectorizedLawsDataset).select($"id", $"topicDistribution")
      .write.format("parquet")
      .save(s"$outputDirPath/laws_LDA_${params.k}_topics_${params.optimizer}_optimizer.parquet")
  }
}
