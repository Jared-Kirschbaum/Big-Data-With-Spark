# Big Data With Spark

## Overview

This project is designed to run a Spark application that processes data using Scala and Spark. The project includes an SBT configuration, a Jupyter notebook for development and experimentation, and a shell script for running the Spark job.

## Project Structure

- `build.sbt`: SBT build configuration file.
- `HW4.ipynb`: Jupyter notebook containing the code and documentation for the project.
- `run.sh`: Shell script to package the application and submit it to a Spark cluster.

## Prerequisites

- Scala 2.12.18
- Spark 2.3.0
- JDK 8 or higher
- SBT (Simple Build Tool) 1.3.13 or higher
- Apache Hadoop/YARN setup for running Spark in cluster mode

## Installation

1. Clone the repository:

    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2. Ensure that SBT is installed. You can download it from [sbt official site](https://www.scala-sbt.org/).

3. Package the project using SBT:

    ```bash
    sbt package
    ```

## Usage

### Running the Spark Job

1. Ensure that Hadoop/YARN is properly configured and running.

2. Use the provided `run.sh` script to submit the Spark job to the cluster:

    ```bash
    sh run.sh
    ```

The `run.sh` script includes the following commands:

    ```bash
    sbt package

    spark-submit \
      --master yarn \
      --deploy-mode cluster \
      --conf spark.executor.instances=5 \
      --conf spark.executor.memory=4g \
      --conf spark.executor.cores=2 \
      --class Q7 \
      ~/spark/target/scala-2.11/hw4_2.11-1.0.jar
    ```

This script will package your application and submit it to a YARN cluster with the specified configurations.

## Project Details

### build.sbt

The `build.sbt` file includes dependencies and build configurations for the project:

    ```scala
    name := "HW4"
    version := "1.0"
    scalaVersion := "2.12.18"

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "2.3.0",
      "org.apache.spark" %% "spark-mllib" % "2.3.0"
    )
    ```

### Jupyter Notebook

The `HW4.ipynb` file contains the main code and analysis for the project. It includes various stages of data processing, analysis, and model building using Spark.

#### Analysis Conducted in Spark

1. **Data Ingestion**: Loading data from HDFS and other data sources into Spark DataFrames.
2. **Data Cleaning and Preprocessing**: Handling missing values, data type conversions, and data transformations.
3. **Exploratory Data Analysis (EDA)**: Using Spark SQL to perform descriptive statistics and visualize data distributions.
4. **Feature Engineering**: Creating new features from raw data to improve model performance.
5. **Model Training**: Training machine learning models using Spark MLlib, including data splitting, model selection, and hyperparameter tuning.
6. **Model Evaluation**: Assessing model performance using metrics like accuracy, precision, recall, and F1-score.
7. **Model Deployment**: Saving the trained model and setting up pipelines for batch or real-time predictions.

## Contributing

Contributions are welcome! Please create a pull request or open an issue to discuss changes.

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.
