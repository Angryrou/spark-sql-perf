package com.databricks.spark.sql.perf

import com.databricks.spark.sql.perf.tpch.TPCH
import com.databricks.spark.sql.perf.ExecutionMode.CollectResults
import com.databricks.spark.sql.perf.tpcds.TPCDS
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SparkSession


case class RunBenchmarkConfig
(
  benchmarkName: String = null, // TPCH / TPCDS
  scaleFactor: String = null, // 1
  locationHeader: String = "hdfs://node13-opa:8020/user/spark_benchmark",
  databaseName: String = null
)

object MyRunBenchmark {

  val iterations = 1
  val timeout = 36*60*60 // timeout, in seconds.

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[RunBenchmarkConfig]("Run-Benchmark-Query") {
      opt[String]('b', "benchmark")
        .action { (x, c) => c.copy(benchmarkName = x) }
        .text("the name of the benchmark to run")
        .required()
      opt[String]('s', "scaleFactor")
        .action((x, c) => c.copy(scaleFactor = x))
        .text("scaleFactor defines the size of the dataset to generate (in GB)")
      opt[String]('l', "locationHeader")
        .action((x, c) => c.copy(locationHeader = x))
        .text("head root directory of location to create data in")
      opt[String]('n', "databaseName")
        .action((x, c) => c.copy(databaseName = x))
        .text("customized databaseName")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, RunBenchmarkConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: RunBenchmarkConfig): Unit = {
    assert(config.benchmarkName == "TPCH" || config.benchmarkName == "TPCDS")

    val sf = config.scaleFactor
    val spark = SparkSession
      .builder()
      .config("spark.sql.shuffle.partitions", if (sf.toInt >= 10000) "20000" else if (sf.toInt >= 1000) "2001" else "200")
      .enableHiveSupport()
      .getOrCreate()

    val databaseName = if (config.databaseName == null) s"${config.benchmarkName.toLowerCase}_${config.scaleFactor}" else config.databaseName
    val resultLocation = s"${config.locationHeader}/${databaseName}/results" // place to write results
//    val databaseName = s"${config.benchmarkName.toLowerCase}_${config.scaleFactor}" // name of database to create.

    spark.sql(s"use $databaseName")

    if (config.benchmarkName == "TPCDS") {
      val tpcds = new TPCDS (sqlContext = spark.sqlContext)
      val queries = tpcds.tpcds2_4Queries
      val experiment = tpcds.runExperiment(
        queries,
        iterations = iterations,
        resultLocation = resultLocation,
        tags = Map("database" -> databaseName, "scale_factor" -> config.scaleFactor)
      )
      experiment.waitForFinish(timeout)
    } else if (config.benchmarkName == "TPCH") {
      val tpch = new TPCH(sqlContext = spark.sqlContext)
      val queries = (1 to 22).map { q =>
        val queryContent: String = IOUtils.toString(
          getClass().getClassLoader().getResourceAsStream(s"tpch/queries/$q.sql"))
        new Query(s"Q$q", spark.sqlContext.sql(queryContent), description = s"TPCH Query $q",
          executionMode = CollectResults)
      }
      val experiment = tpch.runExperiment(
        queries,
        iterations = iterations,
        resultLocation = resultLocation,
        tags=Map("database" -> databaseName, "scale_factor" -> config.scaleFactor))
      experiment.waitForFinish(timeout)
    } else {
      throw new IllegalArgumentException(s"${config.benchmarkName} is not supported.")
    }
  }
}
