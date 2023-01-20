package chenghao.tpcxbb

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object RunSingleQuery {
  def main(args: Array[String]): Unit = {

    val sf = args(0).toInt
    val qid = args(1).toInt
    val vid = args(2).toInt
    val header = args(3).toString

    val db = s"bigbench_sf_$sf"

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    spark.sql(s"use ${db}")
    qid match {
      case 1 => Queries.run_q1(spark, vid, header)
      case 2 => Queries.run_q2(spark, vid)
      case 3 => Queries.run_q3(spark, vid)
      case 4 => Queries.run_q4(spark, vid)
      case 5 => Queries.run_q5(spark, vid)
      case 6 => Queries.run_q6(spark, vid)
      case 7 => Queries.run_q7(spark, vid)
      case 8 => Queries.run_q8(spark, vid)
      case 9 => Queries.run_q9(spark, vid)
      case 10 => Queries.run_q10(spark, vid)
      case 11 => Queries.run_q11(spark, vid)
      case 12 => Queries.run_q12(spark, vid)
      case 13 => Queries.run_q13(spark, vid)
      case 14 => Queries.run_q14(spark, vid)
      case 15 => Queries.run_q15(spark, vid)
      case 16 => Queries.run_q16(spark, vid)
      case 17 => Queries.run_q17(spark, vid)
      case 18 => Queries.run_q18(spark, vid)
      case 19 => Queries.run_q19(spark, vid)
      case 20 => Queries.run_q20(spark, vid)
      case 21 => Queries.run_q21(spark, vid)
      case 22 => Queries.run_q22(spark, vid)
      case 23 => Queries.run_q23(spark, vid)
      case 24 => Queries.run_q24(spark, vid)
      case 25 => Queries.run_q25(spark, vid)
      case 26 => Queries.run_q26(spark, vid)
      case 27 => Queries.run_q27(spark, vid)
      case 28 => Queries.run_q28(spark, vid)
      case 29 => Queries.run_q29(spark, vid)
      case 30 => Queries.run_q30(spark, vid)
    }

    println(spark.sparkContext.applicationId)
    println(spark.sparkContext.getConf.get("spark.yarn.historyServer.address"))

    spark.sqlContext.clearCache()
    spark.stop()

  }
}
