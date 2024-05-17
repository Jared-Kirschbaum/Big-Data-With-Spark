import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, regexp_extract, coalesce, lit, sum}

object Q1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Q1")
      .getOrCreate()

    import spark.implicits._

    // Path to CSV file on HDFS
    val filePath = "hdfs://raleigh.cs.colostate.edu:31640/HW4/data/movie.csv"
    val outputHDFSPath = "hdfs://raleigh.cs.colostate.edu:31640/HW4/q1/"

    // Read the CSV file
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    // Extract the year from the title
    val moviesWithYear = df
      .withColumn("year", regexp_extract(col("title"), "\\(\\d{4}\\)", 0))
      .withColumn("year", expr("substring(year, 2, 4)"))
      .select("title", "movieId", "year")

    // Count the number of movies per year
    val movieCountsPerYear = moviesWithYear
      .groupBy("year")
      .count().alias("count")
      .sort(col("count").desc, col("year"))
      .withColumn("year", coalesce(col("year"), lit("None")))

    // Write the result to HDFS and overwrite existing data if any
    movieCountsPerYear.write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputHDFSPath)

    // Stop the SparkSession
    spark.stop()
  }
}