sbt package

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.executor.instances=5 \
  --conf spark.executor.memory=4g \
  --conf spark.executor.cores=2 \
  --class Q7 \
    ~/spark/target/scala-2.11/hw4_2.11-1.0.jar