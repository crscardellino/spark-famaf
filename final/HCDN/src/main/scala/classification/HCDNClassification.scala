package classification

import java.io.File

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object HCDNClassification {
  private val stringOfColumnValues = udf { array: Seq[String] => array.map(_.trim).mkString("|") }

  private case class Params(
    input: String = "",
    output: String = "")

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Params]("featurization") {
      head("featurization", "1.0")

      opt[String]('i', "input").required().action((input, params) =>
        params.copy(input = input)
      ).validate { input =>
        val datasetDirectory: File = new File(input)
        if (datasetDirectory.exists() && datasetDirectory.isDirectory) success
        else failure("The provided dataset path must a valid directory")
      }.text("Path to the datasets files")

      opt[String]('o', "output").required().action((output, params) =>
        params.copy(output = output)
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

  private def getColumnFeatures(dataset: DataFrame, inputCol: String, outputCol: String): DataFrame = {
    val collectedColumnName = s"${inputCol}Collected"

    val collectedDataset = dataset
      .groupBy("id")
        .agg(collect_set(inputCol).as(collectedColumnName))
      .select(col("id"),
        stringOfColumnValues(col(collectedColumnName)).as(collectedColumnName),
        size(col(collectedColumnName)).as(s"${inputCol}Count"))

    val tokenizer = new RegexTokenizer()
      .setInputCol(collectedColumnName)
      .setOutputCol("columnCategories")
      .setPattern("[^|]+")
      .setGaps(false)

    val tokenizedDataset = tokenizer.transform(collectedDataset)

    val counter = new CountVectorizer()
      .setInputCol("columnCategories")
      .setOutputCol(outputCol)

    counter.fit(tokenizedDataset).transform(tokenizedDataset).drop(collectedColumnName, "columnCategories")
  }

  def run(params: Params): Unit = {
    val inputDatasetDir = new File(params.input).getAbsolutePath
    val outputSaveDir = new File(params.output).getAbsolutePath

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

    // Construction of feature vector

    val partyFeatureVector = getColumnFeatures(signers, "party", "partyFeatureVector")
    val districtFeatureVector = getColumnFeatures(signers, "district", "districtFeatureVector")
    val congressmanFeatureVector = getColumnFeatures(signers, "congressman", "congressmanFeatureVector")
    val deputyCommissionsFeatureVector = getColumnFeatures(deputyCommissions, "commission",
      "deputyCommissionsFeatureVector")

    val extendedSenateCommissions = laws
      .join(senateCommissions, laws("id") === senateCommissions("id"), "left_outer")
      .drop(senateCommissions("id")).na.fill(Map("commission" -> ""))
    val senateCommissionsFeatureVector = getColumnFeatures(extendedSenateCommissions, "commission",
      "senateCommissionsFeatureVector")

    val lawsExtended = laws
      .join(results, laws("id") === results("id"), "left_outer")
      .select(laws("id"), laws("date"),
        concat($"summary", lit(" "), $"law_text").as("text"),
        when($"result" === "SANCIONADO", 1.asInstanceOf[Double]).otherwise(0.asInstanceOf[Double]).as("sanctioned"))
      .join(partyFeatureVector, "id")
      .join(districtFeatureVector, "id")
      .join(congressmanFeatureVector, "id")
      .join(deputyCommissionsFeatureVector.withColumnRenamed("commissionCount", "deputyCommissionsCount"), "id")
      .join(senateCommissionsFeatureVector.withColumnRenamed("commissionCount", "senateCommissionsCount"), "id")

    val tokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("tokens")
      .setPattern("\\p{L}+")
      .setGaps(false)

    val remover = new StopWordsRemover()
      .setStopWords(StopWordsRemover.loadDefaultStopWords("spanish"))
      .setInputCol("tokens")
      .setOutputCol("filteredTokens")

    val hashingTF = new HashingTF()
      .setInputCol("filteredTokens")
      .setOutputCol("hashTokens")
      .setNumFeatures(4096)

    val idf = new IDF()
      .setInputCol("hashTokens")
      .setOutputCol("tfIdf")

    val featureColumns = Array(
      "partyCount", "partyFeatureVector",
      "districtCount", "districtFeatureVector",
      "congressmanCount", "congressmanFeatureVector",
      "deputyCommissionsCount", "deputyCommissionsFeatureVector",
      "senateCommissionsCount", "senateCommissionsFeatureVector",
      "tfIdf"
    )

    val assembler = new VectorAssembler()
      .setInputCols(featureColumns)
      .setOutputCol("featureVector")

    val featurizationPipeline = new Pipeline()
        .setStages(Array(tokenizer, remover, hashingTF, idf, assembler))

    val lawsExtendedDataset = featurizationPipeline
      .fit(lawsExtended)
      .transform(lawsExtended)

    val lawsExtendedPositiveDataset = lawsExtendedDataset.where($"sanctioned" === 1)
    val lawsExtendedNegativeDataset = lawsExtendedDataset.where($"sanctioned" === 0)

    val positiveLawsCount = lawsExtendedPositiveDataset.count.asInstanceOf[Double]
    val negativeLawsCount = lawsExtendedNegativeDataset.count.asInstanceOf[Double]

    val sampleLawsExtendedNegativeDataset = lawsExtendedNegativeDataset
      .sample(false, positiveLawsCount / negativeLawsCount)

    val classificationDataset = lawsExtendedPositiveDataset
      .union(sampleLawsExtendedNegativeDataset)
      .select($"featureVector", $"sanctioned")

    // classification with different algorithms

    val splits = classificationDataset.randomSplit(Array(0.8, 0.2))
    val train = splits(0).cache
    val test = splits(1)

    // Logistic regression classifier
    val LRModel = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.1)
      .setElasticNetParam(0.2)
      .setFeaturesCol("featureVector")
      .setLabelCol("sanctioned")
      .fit(train)

    LRModel.transform(test).select($"sanctioned", $"prediction")
      .write.format("csv")
      .save(s"$outputSaveDir/LRresults.csv")

    // Decision tree classifier
    val DTModel = new DecisionTreeClassifier()
      .setFeaturesCol("featureVector")
      .setLabelCol("sanctioned")
      .fit(train)

    DTModel.transform(test).select($"sanctioned", $"prediction")
      .write.format("csv")
      .save(s"$outputSaveDir/DTresults.csv")

    // Random forest classifier
    val RFModel = new RandomForestClassifier()
      .setFeaturesCol("featureVector")
      .setLabelCol("sanctioned")
      .setNumTrees(10)
      .fit(train)

    RFModel.transform(test).select($"sanctioned", $"prediction")
      .write.format("csv")
      .save(s"$outputSaveDir/RFresults.csv")

    // Naive Bayes classifier
    val NBModel = new NaiveBayes()
      .setFeaturesCol("featureVector")
      .setLabelCol("sanctioned")
      .fit(train)

    NBModel.transform(test).select($"sanctioned", $"prediction")
      .write.format("csv")
      .save(s"$outputSaveDir/NBresults.csv")
  }
}
