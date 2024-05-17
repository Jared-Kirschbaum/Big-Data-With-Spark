import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, size, split, explode, countDistinct}

object Q6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Q6")
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
      .drop("genres_array", "genres")

    // Count the number of movies for each genre
    val movieCountByGenre = explodedDF
      .groupBy("genre")
      .agg(countDistinct("movieId").alias("movie_count"))
      .orderBy("genre")

    // Write the result to HDFS
    val outputHDFSPath = "hdfs://raleigh.cs.colostate.edu:31640/HW4/q6/"
    movieCountByGenre.write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputHDFSPath)

    spark.stop()
  }
}