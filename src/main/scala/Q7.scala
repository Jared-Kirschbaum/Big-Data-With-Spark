import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Q7 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Top Words by Genre")
      .getOrCreate()
    import spark.implicits._

    // Load the movie DataFrame
    val movieDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("hdfs://raleigh.cs.colostate.edu:31640/HW4/data/movie.csv")

    // Explode genres into separate rows for each genre
    val genreDF = movieDF
      .withColumn("genre", explode(split(col("genres"), "\\|")))
      .select("movieId", "title", "genre")

    // Clean title: remove years, punctuation, and normalize spaces
    val cleanedTitlesDF = genreDF
      .withColumn("cleanedTitle", regexp_replace(col("title"), "\\b(18|19|20)\\d{2}\\b", "")) // Remove years like 1997, 2003
      .withColumn("cleanedTitle", regexp_replace(col("cleanedTitle"), "[^\\w\\s]", "")) // Remove punctuation
      .withColumn("cleanedTitle", regexp_replace(col("cleanedTitle"), "\\s+", " ")) // Normalize spaces

    // Tokenize titles
    val tokenizer = new RegexTokenizer()
      .setInputCol("cleanedTitle")
      .setOutputCol("words")
      .setPattern("\\s+") // Split on whitespace

    val tokenizedDF = tokenizer.transform(cleanedTitlesDF)

    // Remove stop words
    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered")
    val removedStopWordsDF = remover.transform(tokenizedDF)

    // Filter out tokens with length <= 3
    val filteredTokensDF = removedStopWordsDF
      .withColumn("filtered", expr("filter(filtered, word -> length(word) > 3)"))

    // Explode words to count and group by genre
    val wordsDF = filteredTokensDF
      .select(col("genre"), explode(col("filtered")).as("word"))
      .groupBy("genre", "word")
      .count()

    // Find top words for each genre
    val windowSpec = Window.partitionBy("genre").orderBy(col("count").desc)
    val topWordsDF = wordsDF
      .withColumn("rank", rank().over(windowSpec))
      .filter(col("rank") <= 10)  // Top 10 words per genre

    // Show or save the result
    topWordsDF.show(false)


    val outputHDFSPath = "hdfs://raleigh.cs.colostate.edu:31640/HW4/q7_top_words/"
    topWordsDF.write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputHDFSPath)

    spark.stop()
  }
}
