package com.databricks.spark.sql.perf

import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpch.TPCHTables
import org.apache.spark.sql.SparkSession


case class GenBenchmarkConfig
(
  benchmarkName: String = null, // TPCH / TPCDS
  dataGenDir: String = "/mnt/disk7/chenghao-dataset",
  scaleFactor: String = null, // 1
  locationHeader: String = "hdfs://node13-opa:8020/user/spark_benchmark",
  overwrite: Boolean = false,
  databaseName: String = null
)

object MySetBenchmark {

  val format = "Parquet"
  val TPCDSUseLegacyOptions = false
  val useDoubleForDecimal = false
  val useStringForDate = false
  val partitionTables = true
  val clusterByPartitionColumns = true
  val filterOutNullPartitionValues = false
  val defaultNumPartitions = 100

  def isPartitioned(tables: Tables, tableName: String): Boolean =
    util.Try(tables.tables.find(_.name == tableName).get.partitionColumns.nonEmpty).getOrElse(false)

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis() //nanoTime()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis() //nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ms")
    result
  }

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[GenBenchmarkConfig]("Gen-Benchmark-data") {
      opt[String]('b', "benchmark")
        .action { (x, c) => c.copy(benchmarkName = x) }
        .text("the name of the benchmark to run")
        .required()
      opt[String]('d', "dataGenDir")
        .action { (x, c) => c.copy(dataGenDir = x) }
        .text("head dir of dsdgen/dbgen")
        .required()
      opt[String]('s', "scaleFactor")
        .action((x, c) => c.copy(scaleFactor = x))
        .text("scaleFactor defines the size of the dataset to generate (in GB)")
      opt[String]('l', "locationHeader")
        .action((x, c) => c.copy(locationHeader = x))
        .text("head root directory of location to create data in")
      opt[Boolean]('o', "overwrite")
        .action((x, c) => c.copy(overwrite = x))
        .text("overwrite the data that is already there")
      help("help")
        .text("prints this usage text")
    }

    parser.parse(args, GenBenchmarkConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: GenBenchmarkConfig): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext

    val tables = config.benchmarkName match {
      case "TPCDS" => (
        new TPCDSTables(spark.sqlContext,
          dsdgenDir = s"${config.dataGenDir}/tpcds-kit/tools",
          scaleFactor = config.scaleFactor,
          useDoubleForDecimal = useDoubleForDecimal,
          useStringForDate = useStringForDate)
        )
      case "TPCH" => (
        new TPCHTables(spark.sqlContext,
          dbgenDir = s"${config.dataGenDir}/dbgen",
          scaleFactor = config.scaleFactor,
          useDoubleForDecimal = useDoubleForDecimal,
          useStringForDate = useStringForDate,
          generatorParams = Nil)
        )
    }

    val databaseName = if (config.databaseName == null) s"${config.benchmarkName.toLowerCase}_${config.scaleFactor}" else config.databaseName
    val location = s"${config.locationHeader}/${databaseName}/dataset"
    val tableNames = tables.tables.map(_.name)
    val workers = sc.getConf.get("spark.executor.instances").toInt
    val cores = sc.getConf.get("spark.executor.cores").toInt

    // name of database to create.

    tableNames.foreach { tableName =>
      // generate data
      time {
        tables.genData(
          location = location,
          format = format,
          overwrite = config.overwrite,
          partitionTables = partitionTables,
          // if to coallesce into a single file (only one writter for non partitioned tables = slow)
          clusterByPartitionColumns = clusterByPartitionColumns, //if (isPartitioned(tables, tableName)) false else true,
          filterOutNullPartitionValues = filterOutNullPartitionValues,
          tableFilter = tableName,
          // this controlls parallelism on datagen and number of writers (# of files for non-partitioned)
          // in general we want many writers to S3, and smaller tasks for large scale factors to avoid OOM and shuffle errors
          numPartitions = if (config.scaleFactor.toInt <= 100 || !isPartitioned(tables, tableName)) (workers * cores)
          else (workers * cores * 4))
      }
    }

    println(s"Creating external tables at $location")
    tables.createExternalTables(location, format, databaseName, overwrite = true, discoverPartitions = true)
    tables.analyzeTables(databaseName, analyzeColumns = true)

  }
}
