import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Q4")
      .getOrCreate()
    import spark.implicits._

    val ratingDf = spark.read
      .option("header", "true")
      .csv("hdfs://raleigh.cs.colostate.edu:31640/HW4/data/rating.csv")
      .withColumnRenamed("movieId", "rating_id")

    val movieDf = spark.read
      .option("header", "true")
      .csv("hdfs://raleigh.cs.colostate.edu:31640/HW4/data/movie.csv")

    val newDf = movieDf
      .join(ratingDf, movieDf("movieId") === ratingDf("rating_id"), "left_outer")

    val genreRatingDf = newDf
      .select(col("movieId"), explode(split(col("genres"), "\\|")).alias("genre"), col("rating"))

    val genreComboRatingDf = genreRatingDf
      .groupBy("movieId")
      .agg(
        collect_set("genre").alias("genre_combo"),
        avg("rating").alias("avg_rating")
      )

    val top3GenreCombos = genreComboRatingDf
      .groupBy("genre_combo")
      .agg(avg("avg_rating").alias("avg_rating"))
      .orderBy(desc("avg_rating"))
      .filter(size(col("genre_combo")) === 3)  // Filter genre combinations of length 3
      .select(
        concat_ws(",", col("genre_combo")).alias("genre_combo"),
        col("avg_rating")
      )

    val outputHDFSPath = "hdfs://raleigh.cs.colostate.edu:31640/HW4/q4/"
    top3GenreCombos.write
      .mode("overwrite")
      .option("header", "true")
      .csv(outputHDFSPath)

    spark.stop()
  }
}