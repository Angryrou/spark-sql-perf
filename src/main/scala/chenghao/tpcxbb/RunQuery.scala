package chenghao.tpcxbb

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object RunQuery {
  def main(args: Array[String]): Unit = {

    val sf = args(0).toInt
    val sleep_time = args(1).toLong

//  args(1): 1-1,1-3,2-1,3-1,3-2,...
    val qv_list = args(2).split(",").toList

    qv_list.foreach(qv => {
      val Array(qid, vid) = qv.split('-').map(_.toInt)
      if (qid > 30 || qid < 1 || vid < 0 || vid > 8) {
        System.err.println("check the workload assignment input")
        System.exit(1)
      }
      if ((qid == 10 || qid == 11) && (vid > 2)) {
        System.err.println("check the workload assignment input for template 10 or template 11")
        System.exit(1)
      }
    })

    val db = s"bigbench_sf_$sf"

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext

    def runQuery(spark: SparkSession, db: String, qid : Int, vid: Int) = {
      spark.sql(s"use $db")
      qid match {
        case 1 => Queries.run_q1(spark, vid)
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
    }

    val ts = new ListBuffer[Long]()
    val t1_list = new ListBuffer[Long]()

    qv_list.foreach(qv => {
      val Array(qid, vid) = qv.split('-').map(_.toInt)

      println(s"---> Here is the statistics for template $qid, variant $vid")

      val t1 = System.currentTimeMillis()
      runQuery(spark, db, qid, vid)
      val t2 = System.currentTimeMillis()
      val dt = t2 - t1
      println(s"---> Query $qid-$vid starts at $t1, costs $dt ms,")
      println()
      ts += dt
      t1_list += t1

      spark.sqlContext.clearCache()
      Thread.sleep(sleep_time * 1000)
    })

    println(s"---> sf: $sf, workload ids <---")
    qv_list.foreach(println)
    println(s"---> sf: $sf, start timestamps <---")
    t1_list.foreach(println)
    println(s"---> sf: $sf, time costs <---")
    ts.foreach(println)

    println(s"---> sf: $sf, csv <---")
    println("trail,start_time,time_cost")
    for (i <- 1 to qv_list.size) {
      println(s"$i,${t1_list(i-1)},${ts(i-1)}")
    }

    spark.stop()

  }
}
