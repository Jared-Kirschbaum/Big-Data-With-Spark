import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, size, split, avg, explode}

object Q3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Q3")
      .getOrCreate()

    import spark.implicits._

    // Read the movie and rating CSV files
    val movieDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("hdfs://raleigh.cs.colostate.edu:31640/HW4/data/movie.csv")
    val ratingDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("hdfs://raleigh.cs.colostate.edu:31640/HW4/data/rating.csv")

    // Join movie and rating dataframes
    val joinedDF = movieDF.join(ratingDF, movieDF("movieId") === ratingDF("movieId"), "left_outer")

    // Split the genres column and create a new column 'genres_array'
    val genresArrayDF = joinedDF.withColumn("genres_array", split(col("genres"), "\\|"))

    // Explode the genres_array column to create a new row for each genre
    val explodedDF = genresArrayDF.withColumn("genre", explode(col("genres_array")))

    // Calculate the average rating for each genre
    val avgRatingByGenre = explodedDF
      .groupBy("genre")
      .agg(avg("rating").alias("avg_rating"))
      .na.drop() // Remove rows with null values

    // Write the result to HDFS
    val outputHDFSPath = "hdfs://raleigh.cs.colostate.edu:31640/HW4/q3/"
    avgRatingByGenre.write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputHDFSPath)

    spark.stop()
  }
}