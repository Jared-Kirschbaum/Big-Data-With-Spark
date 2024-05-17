import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_replace, size, split, explode}

object Q5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Q5")
      .getOrCreate()

    import spark.implicits._

    // Read the movie CSV file
    val movieDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("hdfs://raleigh.cs.colostate.edu:31640/HW4/data/movie.csv")

    // Split the genres column and create a new column 'genres_array'
    val genresArrayDF = movieDF.withColumn("genres_array", split(col("genres"), "\\|"))

    // Explode the 'genres_array' column to create a new row for each genre
    val explodedDF = genresArrayDF
      .withColumn("genre", explode(col("genres_array")))
      .drop("genres_array")

    // Replace all case variations of "comedy" with lowercase "comedy"
    import org.apache.spark.sql.functions.regexp_replace
    val lowercaseGenresDF = explodedDF
      .withColumn("genre", regexp_replace(col("genre"), "(?i)^comedy$", "comedy"))

    // Filter movies with the genre 'comedy'
    val comedyMovies = lowercaseGenresDF.filter(col("genre") === "comedy")

    // Count the number of movies with the genre 'comedy'
    val comedyMoviesCount = comedyMovies.select("movieId").distinct().count()

    // Write the result to HDFS
    val outputHDFSPath = "hdfs://raleigh.cs.colostate.edu:31640/HW4/q5/"
    spark.createDataset(Seq(comedyMoviesCount.toString)).write
      .mode("overwrite")
      .text(outputHDFSPath)

    spark.stop()
  }
}