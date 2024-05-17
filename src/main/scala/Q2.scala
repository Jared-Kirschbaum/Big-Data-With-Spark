import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, size, split, avg}

object Q2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV Reader App")
      .getOrCreate()

    import spark.implicits._

    // Path to file on HDFS
    val filePath = "hdfs://raleigh.cs.colostate.edu:31640/HW4/data/movie.csv"
    val outputHDFSPath = "hdfs://raleigh.cs.colostate.edu:31640/HW4/q2/"

    // Read the CSV file
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    // Split the genres column into an array
    val moviesWithGenres = df
      .withColumn("genres", split(col("genres"), "\\|"))

    // Add a new column 'num_genres' with the length of the genres array
    val moviesWithNumGenres = moviesWithGenres
      .withColumn("num_genres", size(col("genres")))

    // Calculate the average number of genres
    val avgNumGenres = moviesWithNumGenres
      .select(avg(col("num_genres")).alias("avg_num_genres"))

    // Write the result to HDFS and overwrite existing data if any
    avgNumGenres.write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputHDFSPath)

    // Stop the SparkSession
    spark.stop()
  }
}