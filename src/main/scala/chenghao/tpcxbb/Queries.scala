package chenghao.tpcxbb

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegressionModel, NaiveBayes, LogisticRegression => LogisticRegressionSpark}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object Queries {

  def div(dividend: Int, divisor: Int) = {
    val q = dividend / divisor
    val mod = dividend % divisor
    (q, mod)
  }

  /*
  type A template (with 1 parameters) q10 and q11, we expend the previous 3 choices to 20

  type B template (with 2 parameters): 5x8 or 8x5
  - For 5x8, we expend the previous 9 choices to 40 as:
    | *0   | *1   | *2   | 9    | 10   |
    | *3   | *4   | *5   | 11   | 12   |
    | *6   | *7   | *8   | 13   | 14   |
    | 15   | 16   | 17   | 18   | 19   |
    | 20   | ...  |      |      |      |
    | 35   | 36   | 37   | 38   | 39   |

  - For 8x5, we expend the previous one as:
    | *0   | *1   | *2   | 15   | 20   | 35   |
    | *3   | *4   | *5   | 16   | ...  | 36   |
    | *6   | *7   | *8   | 17   |      | 37   |
    | 9    | 11   | 13   | 18   |      | 38   |
    | 10   | 12   | 14   | 19   |      | 39   |
  */


  val type_b_2 = Set(5, 12, 13, 16, 18, 19, 21, 26, 27) // type_b_2 means i1 x i2 = 5 x 8, the reversed version
  val type_b_1_map = Map(9 -> (0, 3), 10 -> (0, 4),
    11 -> (1, 3), 12 -> (1, 4),
    13 -> (2, 3), 14 -> (2, 4)
  )

  def parse_vid(qid: Int, vid: Int) = {
    assert(vid >=0 && vid < 40)
    var i1 = -1
    var i2 = -1
    if (vid < 9) {
      val rets = div(vid, 3)
      i1 = rets._1
      i2 = rets._2
    } else if (vid < 15) {
      val rets = type_b_1_map.getOrElse(vid, (-1, -1))
      i1 = rets._1
      i2 = rets._2
    } else {
      val rets = div(vid, 5)
      i1 = rets._1
      i2 = rets._2
    }
    if (type_b_2.contains(qid)) {
      val tmp = i1
      i1 = i2
      i2 = tmp
    }
    assert(i1 >= 0 && i2 >= 0)
    (i1, i2)
  }

  val run_q1 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(1, vid)
    val q01_limit = Paras.q01_limit_list(i1)
    val q01_i_category_id_IN = Paras.q01_i_category_id_IN_list(i2)

    println(s"The default paramters for template 1 are: q01_limit = ${Paras.q01_limit_list(0)}, " +
      s"q01_i_category_id_IN = ${Paras.q01_i_category_id_IN_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q01_limit = $q01_limit, q01_i_category_id_IN = $q01_i_category_id_IN")
    }

    spark.sql("CREATE TEMPORARY FUNCTION makePairs AS 'io.bigdatabenchmark.v1.queries.udf.PairwiseUDTF'")
    spark.sql(
      s"""
         |SELECT item_sk_1, item_sk_2, COUNT(*) AS cnt
         |FROM
         |(
         |  -- Make item "sold together" pairs
         |  -- combining collect_set + sorting + makePairs(array, selfParing=false)
         |  -- ensures we get no pairs with swapped places like: (12,24),(24,12).
         |  -- We only produce tuples like: (12,24) ensuring that the smaller number is always on the left side
         |  SELECT makePairs(sort_array(itemArray), false) AS (item_sk_1, item_sk_2)
         |  FROM
         |  (
         |    SELECT collect_set(ss_item_sk) AS itemArray --(_list = with duplicates, _set = distinct)
         |    FROM store_sales s, item i
         |    -- Only products in certain categories sold in specific stores are considered,
         |    WHERE s.ss_item_sk = i.i_item_sk
         |    AND i.i_category_id IN (${q01_i_category_id_IN})
         |    AND s.ss_store_sk IN (${Paras.q01_ss_store_sk_IN})
         |    GROUP BY ss_ticket_number
         |  ) soldItemsPerTicket
         |) soldTogetherPairs
         |GROUP BY item_sk_1, item_sk_2
         |-- 'frequently'
         |HAVING cnt > ${Paras.q01_viewed_together_count}
         |ORDER BY cnt DESC, item_sk_1, item_sk_2
         |LIMIT ${q01_limit}
      """.stripMargin).collect()
    //  clean
    spark.sql("DROP TEMPORARY FUNCTION makePairs")
  }

  val run_q2 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(2, vid)
    val q02_limit = Paras.q02_limit_list(i1)
    val q02_item_sk = Paras.q02_item_sk_list(i2)

    println(s"The default paramters for template 2 are: q02_limit = ${Paras.q02_limit_list(0)}, " +
      s"q02_item_sk_list = ${Paras.q02_item_sk_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q02_limit = $q02_limit, q02_item_sk = $q02_item_sk")
    }

    spark.sql("CREATE TEMPORARY FUNCTION makePairs AS 'io.bigdatabenchmark.v1.queries.udf.PairwiseUDTF'")
    val tmp_tbl = "tmp_view"
    val tmp_df = spark.sql(
      s"""
         |SELECT DISTINCT
         |  sessionid,
         |  wcs_item_sk
         |FROM
         |(
         |  FROM
         |  (
         |    SELECT
         |      wcs_user_sk,
         |      wcs_item_sk,
         |      (wcs_click_date_sk * 24 * 60 * 60 + wcs_click_time_sk) AS tstamp_inSec
         |    FROM web_clickstreams
         |    WHERE wcs_item_sk IS NOT NULL
         |    AND   wcs_user_sk IS NOT NULL
         |    DISTRIBUTE BY wcs_user_sk
         |    SORT BY
         |      wcs_user_sk,
         |      tstamp_inSec
         |  ) clicksAnWebPageType
         |  REDUCE
         |    wcs_user_sk,
         |    tstamp_inSec,
         |    wcs_item_sk
         |  USING 'python q2-sessionize.py ${Paras.q02_session_timeout_inSec}'
         |  AS (
         |    wcs_item_sk BIGINT,
         |    sessionid STRING)
         |) q02_tmp_sessionize
         |CLUSTER BY sessionid
      """.stripMargin)
    tmp_df.createOrReplaceTempView(tmp_tbl)
    spark.sql(
      s"""
         |SELECT
         |  item_sk_1,
         |  ${q02_item_sk} AS item_sk_2,
         |  COUNT (*) AS cnt
         |FROM
         |(
         |  SELECT explode(itemArray) AS item_sk_1
         |  FROM
         |  (
         |    SELECT collect_list(wcs_item_sk) AS itemArray
         |    FROM ${tmp_tbl}
         |    GROUP BY sessionid
         |    HAVING array_contains(itemArray, cast(${q02_item_sk} AS BIGINT) )
         |  ) collectedList
         |) pairs
         |WHERE item_sk_1 <> ${q02_item_sk}
         |GROUP BY item_sk_1
         |ORDER BY
         |  cnt DESC,
         |  item_sk_1
         |LIMIT ${q02_limit}
      """.stripMargin).collect()

    //  clean
    spark.sql("DROP TEMPORARY FUNCTION makePairs")
    spark.catalog.dropTempView(tmp_tbl)
  }

  val run_q3 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(3, vid)
    val q03_limit = Paras.q03_limit_list(i1)
    val q03_purchased_item_IN = Paras.q03_purchased_item_IN_list(i2)

    println(s"The default paramters for template 3 are: q03_limit = ${Paras.q03_limit_list(0)}, " +
      s"q03_purchased_item_IN = ${Paras.q03_purchased_item_IN_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q03_limit = $q03_limit, q03_purchased_item_IN = $q03_purchased_item_IN")
    }

    spark.sql(
      s"""
         |SELECT purchased_item, lastviewed_item, COUNT(*) AS cnt
         |FROM
         |(
         |  SELECT *
         |  FROM item i,
         |  ( -- sessionize and filter "last 5 viewed products after purchase of specific item" with reduce script
         |    FROM
         |    (
         |      SELECT
         |        wcs_user_sk,
         |        (wcs_click_date_sk * 24 * 60 * 60 + wcs_click_time_sk) AS tstamp,
         |        wcs_item_sk,
         |        wcs_sales_sk
         |      FROM web_clickstreams w
         |      WHERE wcs_user_sk IS NOT NULL -- only select clickstreams resulting in a purchase (user_sk != null)
         |      AND wcs_item_sk IS NOT NULL
         |      DISTRIBUTE BY wcs_user_sk -- build clickstream per user
         |      SORT BY wcs_user_sk, tstamp ASC, wcs_sales_sk, wcs_item_sk --order by tstamp ASC => required by python script
         |    ) q03_map_output
         |    REDUCE
         |      q03_map_output.wcs_user_sk,
         |      q03_map_output.tstamp,
         |      q03_map_output.wcs_item_sk,
         |      q03_map_output.wcs_sales_sk
         |    -- Reducer script logic: iterate through clicks of a user in ascending order (oldest recent click first).
         |    -- keep a list of the last N clicks and clickdate in a LRU list. if a purchase is found (wcs_sales_sk!=null) display the N previous clicks if they are within the provided date range (max 10 days before purchase)
         |    -- Reducer script selects only:
         |    -- * products viewed within 'q03_days_before_purchase' days before the purchase date
         |    -- * consider only purchase of specific item
         |    -- * only the last 5 products that where viewed before a sale
         |    USING 'python q03_filterLast_N_viewedItmes_within_y_days.py ${Paras.q03_days_in_sec_before_purchase} ${Paras.q03_views_before_purchase} ${q03_purchased_item_IN}'
         |    AS (purchased_item BIGINT, lastviewed_item BIGINT)
         |  ) lastViewSessions
         |  WHERE i.i_item_sk = lastViewSessions.lastviewed_item
         |  AND i.i_category_id IN (${Paras.q03_purchased_item_category_IN}) --Only products in certain categories
         |  CLUSTER BY lastviewed_item,purchased_item -- pre-cluster to speed up following group by and count()
         |) distributed
         |GROUP BY purchased_item,lastviewed_item
         |ORDER BY cnt DESC, purchased_item, lastviewed_item
         |--DISTRIBUTE BY lastviewed_item SORT BY cnt DESC, purchased_item, lastviewed_item --cluster parallel sorting
         |LIMIT ${q03_limit}
      """.stripMargin).collect()
  }

  val run_q4 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(4, vid)
    val q04_wcs_click_date_upper = Paras.q04_wcs_click_date_upper_list(i1)
    val q04_additional_time_pressure_rate = Paras.q04_additional_time_pressure_rate_list(i2)

    println(s"The default paramters for template 4 are: q04_wcs_click_date_upper by default is = None, " +
      s"q04_additional_time_pressure by default = ${Paras.q04_additional_time_pressure_rate_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q04_wcs_click_date_upper = $q04_wcs_click_date_upper, " +
        s"q04_additional_time_pressure_rate = $q04_additional_time_pressure_rate")
    }

    val tmp_tbl = "tmp_tbl"
    val tmp_df = spark.sql(
      s"""
         |FROM
         |(
         |  SELECT
         |    c.wcs_user_sk,
         |    w.wp_type,
         |    (wcs_click_date_sk * 24 * 60 * 60 + wcs_click_time_sk) AS tstamp_inSec
         |  FROM web_clickstreams c, web_page w
         |  WHERE c.wcs_web_page_sk = w.wp_web_page_sk
         |  ${if (i1 > 0) s"AND c.wcs_click_date_sk <= ${q04_wcs_click_date_upper}" else ""}
         |  AND   c.wcs_web_page_sk IS NOT NULL
         |  AND   c.wcs_user_sk     IS NOT NULL
         |  AND   c.wcs_sales_sk    IS NULL --abandoned implies: no sale
         |  DISTRIBUTE BY wcs_user_sk SORT BY wcs_user_sk, tstamp_inSec
         |) clicksAnWebPageType
         |REDUCE
         |  wcs_user_sk,
         |  tstamp_inSec,
         |  wp_type
         |USING 'python q4_sessionize.py ${Paras.q04_session_timeout_inSec} ${q04_additional_time_pressure_rate}'
         |AS (
         |    wp_type STRING,
         |    tstamp BIGINT, --we require timestamp in further processing to keep output deterministic cross multiple reducers
         |    sessionid STRING
         |)
       """.stripMargin)

    tmp_df.createOrReplaceTempView(tmp_tbl)

    spark.sql(
      s"""
         |SELECT SUM(pagecount) / COUNT(*)
         |FROM
         |(
         |  FROM
         |  (
         |    SELECT *
         |    FROM ${tmp_tbl} sessions
         |    DISTRIBUTE BY sessionid SORT BY sessionid, tstamp, wp_type --required by "abandonment analysis script"
         |  ) distributedSessions
         |  REDUCE
         |    wp_type,
         |    --tstamp, --already sorted by time-stamp
         |    sessionid --but we still need the sessionid within the script to identify session boundaries
         |    -- script requires input tuples to be grouped by sessionid and ordered by timestamp ascending.
         |    -- output one tuple: <pagecount> if a session's shopping cart is abandoned, else: nothing
         |    USING 'python q4_abandonedShoppingCarts.py'
         |    AS (pagecount BIGINT)
         |) abandonedShoppingCartsPageCountsPerSession
       """.stripMargin).collect()

    spark.catalog.dropTempView(tmp_tbl)

  }

  val run_q5 = (spark: SparkSession, vid: Int) => {

    // [reversed] i1 x i2 = 5 x 8
    val (i1, i2) = parse_vid(5, vid)
    val q05_i_category = Paras.q05_i_category_list(i1)
    val q05_lambda: String = Paras.q05_lambda_list(i2)

    println(s"The default paramters for template 5 are: q05_i_category = ${Paras.q05_i_category_list(0)}, " +
      s"q05_lambda = ${Paras.q05_lambda_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q05_i_category = $q05_i_category, q05_lambda = $q05_lambda")
    }


    // step 1: extract the input data
    val rawData = spark.sql(
      s"""
         |SELECT
         |  --wcs_user_sk,
         |  clicks_in_category,
         |  CASE WHEN cd_education_status IN (${Paras.q05_cd_education_status_IN}) THEN 1 ELSE 0 END AS college_education,
         |  CASE WHEN cd_gender = ${Paras.q05_cd_gender} THEN 1 ELSE 0 END AS male,
         |  clicks_in_1,
         |  clicks_in_2,
         |  clicks_in_3,
         |  clicks_in_4,
         |  clicks_in_5,
         |  clicks_in_6,
         |  clicks_in_7
         |FROM(
         |  SELECT
         |    wcs_user_sk,
         |    SUM( CASE WHEN i_category = ${q05_i_category} THEN 1 ELSE 0 END) AS clicks_in_category,
         |    SUM( CASE WHEN i_category_id = 1 THEN 1 ELSE 0 END) AS clicks_in_1,
         |    SUM( CASE WHEN i_category_id = 2 THEN 1 ELSE 0 END) AS clicks_in_2,
         |    SUM( CASE WHEN i_category_id = 3 THEN 1 ELSE 0 END) AS clicks_in_3,
         |    SUM( CASE WHEN i_category_id = 4 THEN 1 ELSE 0 END) AS clicks_in_4,
         |    SUM( CASE WHEN i_category_id = 5 THEN 1 ELSE 0 END) AS clicks_in_5,
         |    SUM( CASE WHEN i_category_id = 6 THEN 1 ELSE 0 END) AS clicks_in_6,
         |    SUM( CASE WHEN i_category_id = 7 THEN 1 ELSE 0 END) AS clicks_in_7
         |  FROM web_clickstreams
         |  INNER JOIN item it ON (wcs_item_sk = i_item_sk
         |                     AND wcs_user_sk IS NOT NULL)
         |  GROUP BY  wcs_user_sk
         |)q05_user_clicks_in_cat
         |INNER JOIN customer ct ON wcs_user_sk = c_customer_sk
         |INNER JOIN customer_demographics ON c_current_cdemo_sk = cd_demo_sk
       """.stripMargin)
    rawData.cache()

    // step 2: logistic regression with spark-mllib (DataFrame)
    val options = Map(
      'iter -> "20",
//      'lambda -> "0.0",
      'convergenceTol -> 1e-5.toString
    )

    import spark.implicits._
    val average = rawData.select(mean($"clicks_in_category")).head().getDouble(0)
    val data = rawData.withColumn("label", when($"clicks_in_category" > average, 1.0d).otherwise(0.0d))

    // merge all columns except "clicks_in_category" into one vector
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array(
        "college_education", "male",
        "clicks_in_1", "clicks_in_2", "clicks_in_3", "clicks_in_4", "clicks_in_5", "clicks_in_6", "clicks_in_7"))
      .setOutputCol("features")

    //train model with data using LBFGS LogisticRegression
    val logisticRegression = new LogisticRegressionSpark()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(options('iter).toInt)
      .setRegParam(q05_lambda.toDouble)
      .setTol(options('convergenceTol).toDouble)

    val pipeline = new Pipeline().setStages(Array(vectorAssembler, logisticRegression))

//    println("Training Model")
    val model = pipeline.fit(data)

//    println("Predict with model")
    val predictions = model.transform(data)

    predictions.cache()

    val logitModel = model.stages(model.stages.length - 1).asInstanceOf[LogisticRegressionModel]
    val summary = logitModel.binarySummary
    val multMetrics = new MulticlassMetrics(
      predictions.select($"prediction", $"label").rdd.map(r => (r.getDouble(0), r.getDouble(1)))
    )

    val auc = summary.areaUnderROC
    val prec = multMetrics.weightedPrecision
    val confMat = multMetrics.confusionMatrix

    val metaInformation =
      f"""Precision: $prec%.4f
         |AUC: $auc%.4f
         |Confusion Matrix:
         |$confMat
         |""".stripMargin

    println(metaInformation)

    rawData.unpersist()
    predictions.unpersist()
  }

  val run_q6 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(6, vid)
    val q06_limit = Paras.q06_limit_list(i1)
    val q06_year = Paras.q06_year_list(i2)

    println(s"The default paramters for template 6 are: q06_limit = ${Paras.q06_limit_list(0)}, " +
      s"q06_year = ${Paras.q06_year_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q06_limit = $q06_limit, q06_year = $q06_year")
    }


    val tmp_tbl1 = "tmp_tbl1"
    val tmp_tbl2 = "tmp_tbl2"

    val tmp_df1 = spark.sql(
      s"""
         |SELECT ss_customer_sk AS customer_sk,
         |       sum( case when (d_year = ${q06_year})   THEN (((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2)  ELSE 0 END) first_year_total,
         |       sum( case when (d_year = ${q06_year}+1) THEN (((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2)  ELSE 0 END) second_year_total
         |FROM  store_sales
         |     ,date_dim
         |WHERE ss_sold_date_sk = d_date_sk
         |AND   d_year BETWEEN ${q06_year} AND ${q06_year} +1
         |GROUP BY ss_customer_sk
         |HAVING first_year_total > 0
       """.stripMargin
    )
    tmp_df1.createOrReplaceTempView(tmp_tbl1)

    val tmp_df2 = spark.sql(
      s"""
         |SELECT ws_bill_customer_sk AS customer_sk ,
         |       sum( case when (d_year = ${q06_year})   THEN (((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2)   ELSE 0 END) first_year_total,
         |       sum( case when (d_year = ${q06_year}+1) THEN (((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2)   ELSE 0 END) second_year_total
         |FROM web_sales
         |    ,date_dim
         |WHERE ws_sold_date_sk = d_date_sk
         |AND   d_year BETWEEN ${q06_year} AND ${q06_year} +1
         |GROUP BY ws_bill_customer_sk
         |HAVING first_year_total > 0
       """.stripMargin
    )
    tmp_df2.createOrReplaceTempView(tmp_tbl2)

    spark.sql(
      s"""
         |SELECT
         |      (web.second_year_total / web.first_year_total) AS web_sales_increase_ratio,
         |      c_customer_sk,
         |      c_first_name,
         |      c_last_name,
         |      c_preferred_cust_flag,
         |      c_birth_country,
         |      c_login,
         |      c_email_address
         |FROM ${tmp_tbl1} store,
         |     ${tmp_tbl2} web,
         |     customer c
         |WHERE store.customer_sk = web.customer_sk
         |AND   web.customer_sk = c_customer_sk
         |-- if customer has sales in first year for both store and websales, select him only if web second_year_total/first_year_total ratio is bigger then his store second_year_total/first_year_total ratio.
         |AND   (web.second_year_total / web.first_year_total)  >  (store.second_year_total / store.first_year_total)
         |ORDER BY
         |  web_sales_increase_ratio DESC,
         |  c_customer_sk,
         |  c_first_name,
         |  c_last_name,
         |  c_preferred_cust_flag,
         |  c_birth_country,
         |  c_login
         |LIMIT ${q06_limit}
       """.stripMargin).collect()

    //  clean
    spark.catalog.dropTempView(tmp_tbl1)
    spark.catalog.dropTempView(tmp_tbl2)
  }

  val run_q7 = (spark: SparkSession, vid: Int) => {
    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(7, vid)
    val q07_limit = Paras.q07_limit_list(i1)
    val q07_year = Paras.q07_year_list(i2)

    println(s"The default paramters for template 7 are: q07_limit = ${Paras.q07_limit_list(0)}, " +
      s"q07_year = ${Paras.q07_year_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q07_limit = $q07_limit, q07_year = $q07_year")
    }

    val tmp_tbl = "tmp_tbl"

    val tmp_df = spark.sql(
      s"""
         |SELECT k.i_item_sk
         |FROM item k,
         |(
         |  SELECT
         |    i_category,
         |    AVG(j.i_current_price) * ${Paras.q07_HIGHER_PRICE_RATIO} AS avg_price
         |  FROM item j
         |  GROUP BY j.i_category
         |) avgCategoryPrice
         |WHERE avgCategoryPrice.i_category = k.i_category
         |AND k.i_current_price > avgCategoryPrice.avg_price
       """.stripMargin)
    tmp_df.createOrReplaceTempView(tmp_tbl)
    spark.sql(
      s"""
         |SELECT
         |  ca_state,
         |  COUNT(*) AS cnt
         |FROM
         |  customer_address a,
         |  customer c,
         |  store_sales s,
         |  ${tmp_tbl} highPriceItems
         |WHERE a.ca_address_sk = c.c_current_addr_sk
         |AND c.c_customer_sk = s.ss_customer_sk
         |AND ca_state IS NOT NULL
         |AND ss_item_sk = highPriceItems.i_item_sk --cannot use "ss_item_sk IN ()". Hive only supports a single "IN" subquery per SQL statement.
         |AND s.ss_sold_date_sk IN
         |( --during a given month
         |  SELECT d_date_sk
         |  FROM date_dim
         |  WHERE d_year = ${q07_year}
         |  AND d_moy = ${Paras.q07_MONTH}
         |)
         |GROUP BY ca_state
         |HAVING cnt >= ${Paras.q07_HAVING_COUNT_GE} --at least 10 customers
         |ORDER BY cnt DESC, ca_state --top 10 states in descending order
         |limit ${q07_limit}
       """.stripMargin).collect()


    spark.catalog.dropTempView(tmp_tbl)
  }

  val run_q8 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(8, vid)
    val q08_startDate = Paras.q08_startDate_list(i1)
    val q08_endDate = Paras.q08_endDate_list(i1)
    val q08_seconds_before_purchase = Paras.q08_seconds_before_purchase_list(i2)

    println(s"The default paramters for template 8 are: q08_startDate = ${Paras.q08_startDate_list(0)}, " +
      s"q08_endDate = ${Paras.q08_endDate_list(0)}" +
      s"q08_seconds_before_purchase = ${Paras.q08_seconds_before_purchase_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q08_startDate = $q08_startDate, q08_endDate = $q08_endDate, " +
        s"q08_seconds_before_purchase = $q08_seconds_before_purchase")
    }

    val tmp_tbl1 = "tmp_tbl1"
    val tmp_tbl2 = "tmp_tbl2"
    val tmp_tbl3 = "tmp_tbl3"

    val tmp_df1 = spark.sql(
      s"""
         |SELECT d_date_sk
         |FROM date_dim d
         |WHERE d.d_date >= '${q08_startDate}'
         |AND   d.d_date <= '${q08_endDate}'
       """.stripMargin)
    tmp_df1.createOrReplaceTempView(tmp_tbl1)

    val tmp_df2 = spark.sql(
      s"""
         |SELECT DISTINCT wcs_sales_sk
         |FROM ( -- sessionize clickstreams and filter "viewed reviews" by looking at the web_page page type using a python script
         |  FROM ( -- select only webclicks in relevant time frame and get the type
         |    SELECT  wcs_user_sk,
         |            (wcs_click_date_sk * 86400L + wcs_click_time_sk) AS tstamp_inSec, --every wcs_click_date_sk equals one day => convert to seconds date*24*60*60=date*86400 and add time_sk
         |            wcs_sales_sk,
         |            wp_type
         |    FROM web_clickstreams
         |    LEFT SEMI JOIN ${tmp_tbl1} date_filter ON (wcs_click_date_sk = date_filter.d_date_sk and wcs_user_sk IS NOT NULL)
         |    JOIN web_page w ON wcs_web_page_sk = w.wp_web_page_sk
         |    --WHERE wcs_user_sk IS NOT NULL
         |    DISTRIBUTE BY wcs_user_sk SORT BY wcs_user_sk,tstamp_inSec,wcs_sales_sk,wp_type -- cluster by uid and sort by tstamp required by following python script
         |  ) q08_map_output
         |  -- input: web_clicks in a given year
         |  REDUCE  wcs_user_sk,
         |          tstamp_inSec,
         |          wcs_sales_sk,
         |          wp_type
         |  USING 'python q08_filter_sales_with_reviews_viewed_before.py review ${q08_seconds_before_purchase}'
         |  AS (wcs_sales_sk BIGINT)
         |) sales_which_read_reviews
       """.stripMargin)
    tmp_df2.createOrReplaceTempView(tmp_tbl2)

    val tmp_df3 = spark.sql(
      s"""
         |SELECT ws_net_paid, ws_order_number
         |FROM web_sales ws
         |JOIN ${tmp_tbl1} d ON ( ws.ws_sold_date_sk = d.d_date_sk)
       """.stripMargin)
    tmp_df3.createOrReplaceTempView(tmp_tbl3)

    spark.sql(
      s"""
         |SELECT
         |  q08_review_sales.amount AS q08_review_sales_amount,
         |  q08_all_sales.amount - q08_review_sales.amount AS no_q08_review_sales_amount
         |-- both subqueries only contain a single line with the aggregated sum. Join on 1=1 to get both results into same line for calculating the difference of the two results
         |FROM (
         |  SELECT 1 AS id, SUM(ws_net_paid) as amount
         |  FROM ${tmp_tbl3} allSalesInYear
         |  LEFT SEMI JOIN ${tmp_tbl2} salesWithViewedReviews ON allSalesInYear.ws_order_number = salesWithViewedReviews.wcs_sales_sk
         |) q08_review_sales
         |JOIN (
         |  SELECT 1 AS id, SUM(ws_net_paid) as amount
         |  FROM ${tmp_tbl3} allSalesInYear
         |) q08_all_sales
         |ON q08_review_sales.id = q08_all_sales.id
       """.stripMargin).collect()


    spark.catalog.dropTempView(tmp_tbl1)
    spark.catalog.dropTempView(tmp_tbl2)
    spark.catalog.dropTempView(tmp_tbl3)
  }

  val run_q9 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(9, vid)
    val q09_year = Paras.q09_year_list(i1)
    val q09_part1_marital_status = Paras.q09_marital_status_list(i2)
    val q09_part2_marital_status = Paras.q09_marital_status_list(i2)
    val q09_part3_marital_status = Paras.q09_marital_status_list(i2)

    println(s"The default paramters for template 9 are: q09_year = ${Paras.q09_year_list(0)}, " +
      s"q09_part[1-3]_marital_status = ${Paras.q09_marital_status_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q09_year = $q09_year, q09_part[1-3]_marital_status = $q09_part1_marital_status")
    }

    spark.sql(
      s"""
         |SELECT SUM(ss1.ss_quantity)
         |FROM store_sales ss1, date_dim dd,customer_address ca1 , store s ,customer_demographics cd
         |-- select date range
         |WHERE ss1.ss_sold_date_sk = dd.d_date_sk
         |AND dd.d_year=${q09_year}
         |AND ss1.ss_addr_sk = ca1.ca_address_sk
         |AND s.s_store_sk = ss1.ss_store_sk
         |AND cd.cd_demo_sk = ss1.ss_cdemo_sk
         |AND
         |(
         |  (
         |    cd.cd_marital_status = '${q09_part1_marital_status}'
         |    AND cd.cd_education_status = '${Paras.q09_part1_education_status}'
         |    AND ${Paras.q09_part1_sales_price_min} <= ss1.ss_sales_price
         |    AND ss1.ss_sales_price <= ${Paras.q09_part1_sales_price_max}
         |  )
         |  OR
         |  (
         |    cd.cd_marital_status = '${q09_part2_marital_status}'
         |    AND cd.cd_education_status = '${Paras.q09_part2_education_status}'
         |    AND ${Paras.q09_part2_sales_price_min} <= ss1.ss_sales_price
         |    AND ss1.ss_sales_price <= ${Paras.q09_part2_sales_price_max}
         |  )
         |  OR
         |  (
         |    cd.cd_marital_status = '${q09_part3_marital_status}'
         |    AND cd.cd_education_status = '${Paras.q09_part3_education_status}'
         |    AND ${Paras.q09_part3_sales_price_min} <= ss1.ss_sales_price
         |    AND ss1.ss_sales_price <= ${Paras.q09_part3_sales_price_max}
         |  )
         |)
         |AND
         |(
         |  (
         |    ca1.ca_country = '${Paras.q09_part1_ca_country}'
         |    AND ca1.ca_state IN (${Paras.q09_part1_ca_state_IN})
         |    AND ${Paras.q09_part1_net_profit_min} <= ss1.ss_net_profit
         |    AND ss1.ss_net_profit <= ${Paras.q09_part1_net_profit_max}
         |  )
         |  OR
         |  (
         |    ca1.ca_country = '${Paras.q09_part2_ca_country}'
         |    AND ca1.ca_state IN (${Paras.q09_part2_ca_state_IN})
         |    AND ${Paras.q09_part2_net_profit_min} <= ss1.ss_net_profit
         |    AND ss1.ss_net_profit <= ${Paras.q09_part2_net_profit_max}
         |  )
         |  OR
         |  (
         |    ca1.ca_country = '${Paras.q09_part3_ca_country}'
         |    AND ca1.ca_state IN (${Paras.q09_part3_ca_state_IN})
         |    AND ${Paras.q09_part3_net_profit_min} <= ss1.ss_net_profit
         |    AND ss1.ss_net_profit <= ${Paras.q09_part3_net_profit_max}
         |  )
         |)
       """.stripMargin).collect()
  }

  val run_q10 = (spark: SparkSession, vid: Int) => {

    // q10 only has one parameter. vid = 0 to 19 (included)
    assert(vid >= 0 && vid < 20)
    val q10_limit = Paras.q10_limit_list(vid)

    println(s"The default paramters for template 10 are: q10_limit by default = None")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q10_limit = $q10_limit")
    }

    spark.sql("CREATE TEMPORARY FUNCTION extract_sentiment AS 'io.bigdatabenchmark.v1.queries.q10.SentimentUDF'")
    // ADD LIMIT 1000, 3000, 10000
    spark.sql(
      s"""
        |SELECT item_sk, review_sentence, sentiment, sentiment_word
        |FROM (--wrap in additional FROM(), because Sorting/distribute by with UDTF in select clause is not allowed
        |  SELECT extract_sentiment(pr_item_sk, pr_review_content) AS (item_sk, review_sentence, sentiment, sentiment_word)
        |  FROM product_reviews
        |) extracted
        |ORDER BY item_sk,review_sentence,sentiment,sentiment_word
        |${if (vid > 0) s"LIMIT $q10_limit" else ""}
      """.stripMargin).collect()

    spark.sql("DROP TEMPORARY FUNCTION extract_sentiment")
  }

  val run_q11 = (spark: SparkSession, vid: Int) => {

    // q11 only has one parameter. vid = 0 to 19 (included)
    val q11_startDate=Paras.q11_startDate_list(vid)
    val q11_endDate=Paras.q11_endDate_list(vid)

    println(s"The default paramters for template 11 are: q11_startDate = ${Paras.q11_startDate_list(0)}, " +
      s"q11_endDate = ${Paras.q11_endDate_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q11_startDate = $q11_startDate, q11_endDate = $q11_endDate")
    }

    spark.sql(
      s"""
         |SELECT corr(reviews_count,avg_rating)
         |FROM (
         |  SELECT
         |    p.pr_item_sk AS pid,
         |    p.r_count    AS reviews_count,
         |    p.avg_rating AS avg_rating,
         |    s.revenue    AS m_revenue
         |  FROM (
         |    SELECT
         |      pr_item_sk,
         |      count(*) AS r_count,
         |      avg(pr_review_rating) AS avg_rating
         |    FROM product_reviews
         |    WHERE pr_item_sk IS NOT NULL
         |    --this is GROUP BY 1 in original::same as pr_item_sk here::hive complains anyhow
         |    GROUP BY pr_item_sk
         |  ) p
         |  INNER JOIN (
         |    SELECT
         |      ws_item_sk,
         |      SUM(ws_net_paid) AS revenue
         |    FROM web_sales ws
         |    -- Select date range of interest
         |    LEFT SEMI JOIN (
         |      SELECT d_date_sk
         |      FROM date_dim d
         |      WHERE d.d_date >= '${q11_startDate}'
         |      AND   d.d_date <= '${q11_endDate}'
         |    ) dd ON ( ws.ws_sold_date_sk = dd.d_date_sk )
         |    WHERE ws_item_sk IS NOT null
         |    --this is GROUP BY 1 in original::same as ws_item_sk here::hive complains anyhow
         |    GROUP BY ws_item_sk
         |  ) s
         |  ON p.pr_item_sk = s.ws_item_sk
         |) q11_review_stats
       """.stripMargin).collect()

  }

  val run_q12 = (spark: SparkSession, vid: Int) => {

    // [reversed] i1 x i2 = 5 x 8
    val (i1, i2) = parse_vid(12, vid)
    val q12_i_category_IN = Paras.q12_i_category_IN_list(i1)
    val q12_limit = Paras.q12_limit_list(i2)

    println(s"The default paramters for template 12 are: q12_i_category_IN = ${Paras.q12_i_category_IN_list(0)}, " +
      s"q12_limit by default = None")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q12_i_category_IN = $q12_i_category_IN, q12_limit = $q12_limit")
    }

    spark.sql(
      s"""
         |SELECT DISTINCT wcs_user_sk -- Find all customers
         |-- TODO check if 37134 is first day of the month
         |FROM
         |( -- web_clicks viewed items in date range with items from specified categories
         |  SELECT
         |    wcs_user_sk,
         |    wcs_click_date_sk
         |  FROM web_clickstreams, item
         |  WHERE wcs_click_date_sk BETWEEN 37134 AND (37134 + 30) -- in a given month and year
         |  AND i_category IN (${q12_i_category_IN}) -- filter given category
         |  AND wcs_item_sk = i_item_sk
         |  AND wcs_user_sk IS NOT NULL
         |  AND wcs_sales_sk IS NULL --only views, not purchases
         |) webInRange,
         |(  -- store sales in date range with items from specified categories
         |  SELECT
         |    ss_customer_sk,
         |    ss_sold_date_sk
         |  FROM store_sales, item
         |  WHERE ss_sold_date_sk BETWEEN 37134 AND (37134 + 90) -- in the three consecutive months.
         |  AND i_category IN (${q12_i_category_IN}) -- filter given category
         |  AND ss_item_sk = i_item_sk
         |  AND ss_customer_sk IS NOT NULL
         |) storeInRange
         |-- join web and store
         |WHERE wcs_user_sk = ss_customer_sk
         |AND wcs_click_date_sk < ss_sold_date_sk -- buy AFTER viewed on website
         |ORDER BY wcs_user_sk
         |${if (i2 > 0) s"LIMIT ${q12_limit}" else ""}
       """.stripMargin).collect()
  }

  val run_q13 = (spark: SparkSession, vid: Int) => {

    // [reversed] i1 x i2 = 5 x 8
    val (i1, i2) = parse_vid(13, vid)
    val q13_year = Paras.q13_year_list(i1)
    val q13_limit = Paras.q13_limit_list(i2)

    println(s"The default paramters for template 13 are: q13_year = ${Paras.q13_year_list(0)}, " +
      s"q13_limit = ${Paras.q13_limit_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q13_year = $q13_year, q13_limit = $q13_limit")
    }

    val tmp_tbl1 = "tmp_tbl1"
    val tmp_tbl2 = "tmp_tbl2"

    val tmp_df1 = spark.sql(
      s"""
         |SELECT
         |    ss.ss_customer_sk AS customer_sk,
         |    sum( case when (d_year = ${q13_year})   THEN ss_net_paid  ELSE 0 END) first_year_total,
         |    sum( case when (d_year = ${q13_year}+1) THEN ss_net_paid  ELSE 0 END) second_year_total
         |FROM store_sales ss
         |JOIN (
         |  SELECT d_date_sk, d_year
         |  FROM date_dim d
         |  WHERE d.d_year in (${q13_year}, (${q13_year} + 1))
         |) dd on ( ss.ss_sold_date_sk = dd.d_date_sk )
         |GROUP BY ss.ss_customer_sk
         |HAVING first_year_total > 0
       """.stripMargin)
    tmp_df1.createOrReplaceTempView(tmp_tbl1)

    val tmp_df2 = spark.sql(
      s"""
         |SELECT
         |       ws.ws_bill_customer_sk AS customer_sk,
         |       sum( case when (d_year = ${q13_year})   THEN ws_net_paid  ELSE 0 END) first_year_total,
         |       sum( case when (d_year = ${q13_year}+1) THEN ws_net_paid  ELSE 0 END) second_year_total
         |FROM web_sales ws
         |JOIN (
         |  SELECT d_date_sk, d_year
         |  FROM date_dim d
         |  WHERE d.d_year in (${q13_year}, (${q13_year} + 1) )
         |) dd ON ( ws.ws_sold_date_sk = dd.d_date_sk )
         |GROUP BY ws.ws_bill_customer_sk
         |HAVING first_year_total > 0
       """.stripMargin)
    tmp_df2.createOrReplaceTempView(tmp_tbl2)

    spark.sql(
      s"""
         |SELECT
         |      c_customer_sk,
         |      c_first_name,
         |      c_last_name,
         |      (store.second_year_total / store.first_year_total) AS storeSalesIncreaseRatio ,
         |      (web.second_year_total / web.first_year_total) AS webSalesIncreaseRatio
         |FROM ${tmp_tbl1} store ,
         |     ${tmp_tbl2} web ,
         |     customer c
         |WHERE store.customer_sk = web.customer_sk
         |AND   web.customer_sk = c_customer_sk
         |-- if customer has sales in first year for both store and websales, select him only if web second_year_total/first_year_total ratio is bigger then his store second_year_total/first_year_total ratio.
         |AND   (web.second_year_total / web.first_year_total)  >  (store.second_year_total / store.first_year_total)
         |ORDER BY
         |  webSalesIncreaseRatio DESC,
         |  c_customer_sk,
         |  c_first_name,
         |  c_last_name
         |LIMIT ${q13_limit}
       """.stripMargin).collect()

    spark.catalog.dropTempView(tmp_tbl1)
    spark.catalog.dropTempView(tmp_tbl2)
  }

  val run_q14 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(14, vid)
    val q14_dependents = Paras.q14_dependents_list(i1)
    val q14_content_len_min = Paras.q14_content_len_min_list(i2)
    val q14_content_len_max = Paras.q14_content_len_max_list(i2)

    println(s"The default paramters for template 14 are: q14_dependents = ${Paras.q14_dependents_list(0)}, " +
      s"q14_content_len_min = ${Paras.q14_content_len_min_list(0)}, q14_content_len_max = ${Paras.q14_content_len_max_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q14_dependents = $q14_dependents, q14_content_len_min = $q14_content_len_min, " +
        s"q14_content_len_max = $q14_content_len_max")
    }

    spark.sql(
      s"""
         |SELECT CAST(amc as double) / CAST(pmc as double) am_pm_ratio
         |FROM (
         |  SELECT COUNT(*) amc
         |  FROM web_sales ws
         |  JOIN household_demographics hd ON hd.hd_demo_sk = ws.ws_ship_hdemo_sk
         |  AND hd.hd_dep_count = ${q14_dependents}
         |  JOIN time_dim td ON td.t_time_sk = ws.ws_sold_time_sk
         |  AND td.t_hour >= ${Paras.q14_morning_startHour}
         |  AND td.t_hour <= ${Paras.q14_morning_endHour}
         |  JOIN web_page wp ON wp.wp_web_page_sk = ws.ws_web_page_sk
         |  AND wp.wp_char_count >= ${q14_content_len_min}
         |  AND wp.wp_char_count <= ${q14_content_len_max}
         |) at
         |JOIN (
         |  SELECT COUNT(*) pmc
         |  FROM web_sales ws
         |  JOIN household_demographics hd ON ws.ws_ship_hdemo_sk = hd.hd_demo_sk
         |  AND hd.hd_dep_count = ${q14_dependents}
         |  JOIN time_dim td ON td.t_time_sk = ws.ws_sold_time_sk
         |  AND td.t_hour >= ${Paras.q14_evening_startHour}
         |  AND td.t_hour <= ${Paras.q14_evening_endHour}
         |  JOIN web_page wp ON wp.wp_web_page_sk = ws.ws_web_page_sk
         |  AND wp.wp_char_count >= ${q14_content_len_min}
         |  AND wp.wp_char_count <= ${q14_content_len_max}
         |) pt
       """.stripMargin).collect()
  }

  val run_q15 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(15, vid)
    val q15_startDate = Paras.q15_startDate_list(i1)
    val q15_endDate = Paras.q15_endDate_list(i1)
    val q15_store_sk = Paras.q15_store_sk_list(i2)

    println(s"The default paramters for template 15 are: q15_startDate = ${Paras.q15_startDate_list(0)}, " +
      s"q15_endDate = ${Paras.q15_endDate_list(0)}, q15_store_sk = ${Paras.q15_store_sk_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q15_startDate = $q15_startDate, q15_endDate = $q15_endDate, q15_store_sk = $q15_store_sk")
    }

    spark.sql(
      s"""
         |SELECT *
         |FROM (
         |  SELECT
         |    cat,
         |    ((count(x) * SUM(xy) - SUM(x) * SUM(y)) / (count(x) * SUM(xx) - SUM(x) * SUM(x)) ) AS slope,
         |    (SUM(y) - ((count(x) * SUM(xy) - SUM(x) * SUM(y)) / (count(x) * SUM(xx) - SUM(x)*SUM(x)) ) * SUM(x)) / count(x) AS intercept
         |  FROM (
         |    SELECT
         |      i.i_category_id AS cat, -- ranges from 1 to 10
         |      s.ss_sold_date_sk AS x,
         |      SUM(s.ss_net_paid) AS y,
         |      s.ss_sold_date_sk * SUM(s.ss_net_paid) AS xy,
         |      s.ss_sold_date_sk * s.ss_sold_date_sk AS xx
         |    FROM store_sales s
         |    -- select date range
         |    LEFT SEMI JOIN (
         |      SELECT d_date_sk
         |      FROM date_dim d
         |      WHERE d.d_date >= '${q15_startDate}'
         |      AND   d.d_date <= '${q15_endDate}'
         |    ) dd ON ( s.ss_sold_date_sk=dd.d_date_sk )
         |    INNER JOIN item i ON s.ss_item_sk = i.i_item_sk
         |    WHERE i.i_category_id IS NOT NULL
         |    AND s.ss_store_sk = ${q15_store_sk} -- for a given store ranges from 1 to 12
         |    GROUP BY i.i_category_id, s.ss_sold_date_sk
         |  ) temp
         |  GROUP BY cat
         |) regression
         |WHERE slope <= 0
         |ORDER BY cat
       """.stripMargin).collect()
  }

  val run_q16 = (spark: SparkSession, vid: Int) => {

    // [reversed] i1 x i2 = 5 x 8
    val (i1, i2) = parse_vid(16, vid)
    val q16_date = Paras.q16_date_list(i1)
    val q16_limit = Paras.q16_limit_list(i2)

    println(s"The default paramters for template 16 are: q16_date = ${Paras.q16_date_list(0)}, " +
      s"q16_limit = ${Paras.q16_limit_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q16_date = $q16_date, q16_limit = $q16_limit")
    }

    spark.sql(
      s"""
         |SELECT w_state, i_item_id,
         |  SUM(
         |    CASE WHEN (unix_timestamp(d_date,'yyyy-MM-dd') < unix_timestamp('${q16_date}','yyyy-MM-dd'))
         |    THEN ws_sales_price - COALESCE(wr_refunded_cash,0)
         |    ELSE 0.0 END
         |  ) AS sales_before,
         |  SUM(
         |    CASE WHEN (unix_timestamp(d_date,'yyyy-MM-dd') >= unix_timestamp('${q16_date}','yyyy-MM-dd'))
         |    THEN ws_sales_price - COALESCE(wr_refunded_cash,0)
         |    ELSE 0.0 END
         |  ) AS sales_after
         |FROM (
         |  SELECT *
         |  FROM web_sales ws
         |  LEFT OUTER JOIN web_returns wr ON (ws.ws_order_number = wr.wr_order_number
         |    AND ws.ws_item_sk = wr.wr_item_sk)
         |) a1
         |JOIN item i ON a1.ws_item_sk = i.i_item_sk
         |JOIN warehouse w ON a1.ws_warehouse_sk = w.w_warehouse_sk
         |JOIN date_dim d ON a1.ws_sold_date_sk = d.d_date_sk
         |AND unix_timestamp(d.d_date, 'yyyy-MM-dd') >= unix_timestamp('${q16_date}', 'yyyy-MM-dd') - 30*24*60*60 --subtract 30 days in seconds
         |AND unix_timestamp(d.d_date, 'yyyy-MM-dd') <= unix_timestamp('${q16_date}', 'yyyy-MM-dd') + 30*24*60*60 --add 30 days in seconds
         |GROUP BY w_state,i_item_id
         |--original was ORDER BY w_state,i_item_id , but CLUSTER BY is hives cluster scale counter part
         |ORDER BY w_state,i_item_id
         |LIMIT ${q16_limit}
       """.stripMargin).collect()
  }

  val run_q17 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(17, vid)
    val q17_month = Paras.q17_month_list(i1)
    val q17_i_category_IN = Paras.q17_i_category_IN_list(i2)

    println(s"The default paramters for template 17 are: q17_month = ${Paras.q17_month_list(0)}, " +
      s"q17_i_category_IN = ${Paras.q17_i_category_IN_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q17_month = $q17_month, q17_i_category_IN = $q17_i_category_IN")
    }

    spark.sql(
      s"""
         |SELECT promotions, total, promotions / total * 100
         |FROM (
         |  SELECT SUM(ss_ext_sales_price) promotions
         |  FROM store_sales ss
         |  JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
         |  JOIN item i ON ss.ss_item_sk = i.i_item_sk
         |  JOIN store s ON ss.ss_store_sk = s.s_store_sk
         |  JOIN promotion p ON ss.ss_promo_sk = p.p_promo_sk
         |  JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
         |  JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
         |  WHERE ca_gmt_offset = ${Paras.q17_gmt_offset}
         |  AND s_gmt_offset = ${Paras.q17_gmt_offset}
         |  AND i_category IN (${q17_i_category_IN})
         |  AND d_year = ${Paras.q17_year}
         |  AND d_moy = ${q17_month}
         |  AND (p_channel_dmail = 'Y' OR p_channel_email = 'Y' OR p_channel_tv = 'Y')
         |) promotional_sales
         |JOIN (
         |  SELECT SUM(ss_ext_sales_price) total
         |  FROM store_sales ss
         |  JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
         |  JOIN item i ON ss.ss_item_sk = i.i_item_sk
         |  JOIN store s ON ss.ss_store_sk = s.s_store_sk
         |  JOIN promotion p ON ss.ss_promo_sk = p.p_promo_sk
         |  JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
         |  JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
         |  WHERE ca_gmt_offset = ${Paras.q17_gmt_offset}
         |  AND s_gmt_offset = ${Paras.q17_gmt_offset}
         |  AND i_category IN (${q17_i_category_IN})
         |  AND d_year = ${Paras.q17_year}
         |  AND d_moy = ${q17_month}
         |) all_sales
         |-- we don't need a 'ON' join condition. result is just two numbers.
         |ORDER BY promotions, total
         |LIMIT 100
       """.stripMargin).collect()

  }

  val run_q18 = (spark: SparkSession, vid: Int) => {

    // [reversed] i1 x i2 = 5 x 8
    val (i1, i2) = parse_vid(18, vid)
    val q18_startDate = Paras.q18_startDate_list(i1)
    val q18_endDate = Paras.q18_endDate_list(i1)
    val q18_limit = Paras.q18_limit_list(i2)

    println(s"The default paramters for template 18 are: q18_startDate = ${Paras.q18_startDate_list(0)}, " +
      s"q18_endDate = ${Paras.q18_endDate_list(0)}, " +
      s"q18_limit by default = None")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q18_startDate = $q18_startDate, q18_endDate = $q18_endDate, " +
        s"q18_limit = $q18_limit")
    }


    spark.sql("CREATE TEMPORARY FUNCTION extract_NegSentiment AS 'io.bigdatabenchmark.v1.queries.q18.NegativeSentimentUDF'")

    val tmp_tbl1 = "tmp_tbl1"
    val tmp_tbl2 = "tmp_tbl2"

    val tmp_df1 = spark.sql(
      s"""
         |  SELECT s.s_store_sk, s.s_store_name
         |  FROM store s,
         |  (
         |    --select ss_store_sk's with flat or declining sales in 3 consecutive months.
         |    -- linear regression part of stores by analysing store_sales
         |    SELECT
         |      temp.ss_store_sk,
         |      ((count(temp.x) * SUM(temp.xy) - SUM(temp.x) * SUM(temp.y)) / (count(temp.x) * SUM(temp.xx) - SUM(temp.x) * SUM(temp.x)) ) AS slope
         |    FROM
         |    (
         |      SELECT
         |        s.ss_store_sk,
         |        s.ss_sold_date_sk AS x,
         |        SUM(s.ss_net_paid) AS y,
         |        s.ss_sold_date_sk * SUM(s.ss_net_paid) AS xy,
         |        s.ss_sold_date_sk*s.ss_sold_date_sk AS xx
         |      FROM store_sales s
         |      --select date range
         |      LEFT SEMI JOIN (
         |        SELECT d_date_sk
         |        FROM date_dim d
         |        WHERE d.d_date >= '${q18_startDate}'
         |        AND   d.d_date <= '${q18_endDate}'
         |      ) dd ON ( s.ss_sold_date_sk=dd.d_date_sk )
         |      GROUP BY s.ss_store_sk, s.ss_sold_date_sk
         |    ) temp
         |    GROUP BY temp.ss_store_sk
         |  ) regression_analysis
         |  WHERE slope <= 0--flat or declining sales
         |  AND s.s_store_sk = regression_analysis.ss_store_sk
       """.stripMargin)
    tmp_df1.createOrReplaceTempView(tmp_tbl1)

    tmp_df1.cache()
    tmp_df1.collect()

    val tmp_df2 = spark.sql(
      s"""
         |SELECT
         |  CONCAT(s_store_sk,"_", s_store_name ) AS store_ID, --this could be any string we want to identify the store. but as store_sk is just a number and store_name is ambiguous we choose the concatenation of both
         |  pr_review_date,
         |  pr_review_content
         |-- THIS IS A CROSS JOIN! but fortunately the "stores_with_regression" table is very small
         |FROM product_reviews pr, ${tmp_tbl1} stores_with_regression
         |WHERE locate(lower(stores_with_regression.s_store_name), lower(pr.pr_review_content), 1) >= 1 --find store name in reviews
       """.stripMargin)
    tmp_df2.createOrReplaceTempView(tmp_tbl2)

    spark.sql(
      s"""
         |SELECT s_name, r_date, r_sentence, sentiment, sentiment_word
         |FROM ( --wrap in additional FROM(), because Sorting/distribute by with UDTF in select clause is not allowed
         |  --negative sentiment analysis of found reviews for stores with declining sales
         |  SELECT extract_NegSentiment(store_ID, pr_review_date, pr_review_content) AS ( s_name, r_date, r_sentence, sentiment, sentiment_word )
         |  FROM ${tmp_tbl2}
         |) extracted
         |ORDER BY s_name, r_date, r_sentence, sentiment,sentiment_word
         |${if (i2 > 0) s"LIMIT ${q18_limit}" else ""}
       """.stripMargin).collect()

    // clean
    spark.sql("DROP TEMPORARY FUNCTION extract_NegSentiment")
    spark.catalog.dropTempView(tmp_tbl1)
    spark.catalog.dropTempView(tmp_tbl2)
    tmp_df1.unpersist()

  }

  val run_q19 = (spark: SparkSession, vid: Int) => {

    // [reversed] i1 x i2 = 5 x 8
    val (i1, i2) = parse_vid(19, vid)
    val q19_storeReturns_date_IN = Paras.q19_storeReturns_date_IN_list(i1)
    val q19_webReturns_date_IN = Paras.q19_webReturns_date_IN_list(i1)
    val q19_store_return_limit = Paras.q19_store_return_limit_list(i2)

    println(s"The default paramters for template 19 are: q19_storeReturns_date_IN = ${Paras.q19_storeReturns_date_IN_list(0)}, " +
      s"q19_webReturns_date_IN = ${Paras.q19_webReturns_date_IN_list(0)}, +" +
      s"q19_store_return_limit by default = None")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q19_storeReturns_date_IN = $q19_storeReturns_date_IN, q19_webReturns_date_IN = $q19_webReturns_date_IN, " +
        s"q19_store_return_limit = $q19_store_return_limit")
    }

    spark.sql("CREATE TEMPORARY FUNCTION extract_sentiment AS 'io.bigdatabenchmark.v1.queries.q10.SentimentUDF'")
    spark.sql(
      s"""
         |SELECT *
         |FROM
         |( --wrap in additional FROM(), because Sorting/distribute by with UDTF in select clause is not allowed
         |  SELECT extract_sentiment(pr.pr_item_sk, pr.pr_review_content) AS
         |  (
         |    item_sk,
         |    review_sentence,
         |    sentiment,
         |    sentiment_word
         |  )
         |  FROM product_reviews pr,
         |  (
         |    --store returns in week ending given date
         |    SELECT sr_item_sk, SUM(sr_return_quantity) sr_item_qty
         |    FROM store_returns sr,
         |    (
         |      -- within the week ending a given date
         |      SELECT d1.d_date_sk
         |      FROM date_dim d1, date_dim d2
         |      WHERE d1.d_week_seq = d2.d_week_seq
         |      AND d2.d_date IN ( ${q19_storeReturns_date_IN} )
         |    ) sr_dateFilter
         |    WHERE sr.sr_returned_date_sk = d_date_sk
         |    GROUP BY sr_item_sk --across all store and web channels
         |    HAVING sr_item_qty > 0
         |  ) fsr,
         |  (
         |    --web returns in week ending given date
         |    SELECT wr_item_sk, SUM(wr_return_quantity) wr_item_qty
         |    FROM web_returns wr,
         |    (
         |      -- within the week ending a given date
         |      SELECT d1.d_date_sk
         |      FROM date_dim d1, date_dim d2
         |      WHERE d1.d_week_seq = d2.d_week_seq
         |      AND d2.d_date IN ( ${q19_webReturns_date_IN} )
         |    ) wr_dateFilter
         |    WHERE wr.wr_returned_date_sk = d_date_sk
         |    GROUP BY wr_item_sk  --across all store and web channels
         |    HAVING wr_item_qty > 0
         |  ) fwr
         |  WHERE fsr.sr_item_sk = fwr.wr_item_sk
         |  AND pr.pr_item_sk = fsr.sr_item_sk --extract product_reviews for found items
         |  -- equivalent across all store and web channels (within a tolerance of +/- 10%)
         |  AND abs( (sr_item_qty-wr_item_qty)/ ((sr_item_qty+wr_item_qty)/2)) <= 0.1
         |)extractedSentiments
         |WHERE sentiment= 'NEG' --if there are any major negative reviews.
         |--item_sk is skewed, but we need to sort by it. Technically we just expect a deterministic global sorting and not clustering by item_sk...so we could distribute by pr_review_sk
         |ORDER BY item_sk,review_sentence,sentiment,sentiment_word
         |${if (i2 > 0) s"LIMIT ${q19_store_return_limit}" else ""}
       """.stripMargin).collect()
    // clean
    spark.sql("DROP TEMPORARY FUNCTION extract_sentiment")

  }

  val run_q20 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(20, vid)
    val q20_numclust: String = Paras.q20_numclust_list(i1)
    val q20_iter: String = Paras.q20_iter_list(i2)

    println(s"The default paramters for template 20 are: q20_numclust = ${Paras.q20_numclust_list(0)}, " +
      s"q20_iter = ${Paras.q20_iter_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q20_numclust = $q20_numclust, q20_iter = $q20_iter")
    }


    // step 1: extract the input data
    val data = spark.sql(
      s"""
         |SELECT
         |  ss_customer_sk AS user_sk,
         |  round(CASE WHEN ((returns_count IS NULL) OR (orders_count IS NULL) OR ((returns_count / orders_count) IS NULL) ) THEN 0.0 ELSE (returns_count / orders_count) END, 7) AS orderRatio,
         |  round(CASE WHEN ((returns_items IS NULL) OR (orders_items IS NULL) OR ((returns_items / orders_items) IS NULL) ) THEN 0.0 ELSE (returns_items / orders_items) END, 7) AS itemsRatio,
         |  round(CASE WHEN ((returns_money IS NULL) OR (orders_money IS NULL) OR ((returns_money / orders_money) IS NULL) ) THEN 0.0 ELSE (returns_money / orders_money) END, 7) AS monetaryRatio,
         |  round(CASE WHEN ( returns_count IS NULL                                                                        ) THEN 0.0 ELSE  returns_count                 END, 0) AS frequency
         |FROM
         |  (
         |    SELECT
         |      ss_customer_sk,
         |      -- return order ratio
         |      COUNT(distinct(ss_ticket_number)) AS orders_count,
         |      -- return ss_item_sk ratio
         |      COUNT(ss_item_sk) AS orders_items,
         |      -- return monetary amount ratio
         |      SUM( ss_net_paid ) AS orders_money
         |    FROM store_sales s
         |    GROUP BY ss_customer_sk
         |  ) orders
         |  LEFT OUTER JOIN
         |  (
         |    SELECT
         |      sr_customer_sk,
         |      -- return order ratio
         |      count(distinct(sr_ticket_number)) as returns_count,
         |      -- return ss_item_sk ratio
         |      COUNT(sr_item_sk) as returns_items,
         |      -- return monetary amount ratio
         |      SUM( sr_return_amt ) AS returns_money
         |    FROM store_returns
         |    GROUP BY sr_customer_sk
         |  ) returned ON ss_customer_sk=sr_customer_sk
         |ORDER BY user_sk
       """.stripMargin)
    data.cache()

    // step 2: Calculating KMeans with spark (DataFrame)
//    var options = Map(
//      'numclust -> "8",
//      'iter -> "20"
//    )
    import spark.implicits._

    // vectorize all columns except the first column
    // (a, b, c, d, e) => (a, [b, c, d, e])
    val assembler = new VectorAssembler()
      .setInputCols(data.columns.tail)
      .setOutputCol("features")

    val kmeans = new KMeans()
      .setFeaturesCol(assembler.getOutputCol)
      .setK(q20_numclust.toInt)
      .setMaxIter(q20_iter.toInt)
      .setInitMode("k-means||")
      .setSeed(1234567890)

    val pipeline = new Pipeline()
      .setStages(Array(assembler, kmeans))

    val model = pipeline.fit(data)

    // result of the transformation and clustering
    val predictions = model.transform(data)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)

    // get the KMeansModel for meta information
    val kMeansModel = model.stages(1).asInstanceOf[KMeansModel]

    // Evaluate clusterModel by computing Within Set Sum of Squared Errors
    val clusterCenters = kMeansModel.clusterCenters

    //print and write clustering  metadata
    val metaInformation =
      s"""Clusters:
         |
         |Number of Clusters: ${clusterCenters.length}
         |silhouette: $silhouette
         |${clusterCenters.mkString("\n")}
       """.stripMargin
    println(metaInformation)

    // step 3: clean
    data.unpersist()
  }

  val run_q21 = (spark: SparkSession, vid: Int) => {

    // [reversed] i1 x i2 = 5 x 8
    val (i1, i2) = parse_vid(21, vid)
    val q21_year = Paras.q21_year_list(i1)
    val q21_limit = Paras.q21_limit_list(i2)

    println(s"The default paramters for template 21 are: q21_year = ${Paras.q21_year_list(0)}, " +
      s"q21_limit = ${Paras.q21_limit_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q21_year = $q21_year, q21_limit = $q21_limit")
    }


    spark.sql(
      s"""
         |SELECT
         |  part_i.i_item_id AS i_item_id,
         |  part_i.i_item_desc AS i_item_desc,
         |  part_s.s_store_id AS s_store_id,
         |  part_s.s_store_name AS s_store_name,
         |  SUM(part_ss.ss_quantity) AS store_sales_quantity,
         |  SUM(part_sr.sr_return_quantity) AS store_returns_quantity,
         |  SUM(part_ws.ws_quantity) AS web_sales_quantity
         |FROM (
         |	SELECT
         |	  sr_item_sk,
         |	  sr_customer_sk,
         |	  sr_ticket_number,
         |	  sr_return_quantity
         |	FROM
         |	  store_returns sr,
         |	  date_dim d2
         |	WHERE d2.d_year = ${q21_year}
         |	AND d2.d_moy BETWEEN ${Paras.q21_month} AND ${Paras.q21_month} + 6 --which were returned in the next six months
         | 	AND sr.sr_returned_date_sk = d2.d_date_sk
         |) part_sr
         |INNER JOIN (
         |  SELECT
         |    ws_item_sk,
         |    ws_bill_customer_sk,
         |    ws_quantity
         |  FROM
         |    web_sales ws,
         |    date_dim d3
         |  WHERE d3.d_year BETWEEN ${q21_year} AND ${q21_year} + 2 -- in the following three years (re-purchased by the returning customer afterwards through the web sales channel)
         |  AND ws.ws_sold_date_sk = d3.d_date_sk
         |) part_ws ON (
         |  part_sr.sr_item_sk = part_ws.ws_item_sk
         |  AND part_sr.sr_customer_sk = part_ws.ws_bill_customer_sk
         |)
         |INNER JOIN (
         |  SELECT
         |    ss_item_sk,
         |    ss_store_sk,
         |    ss_customer_sk,
         |    ss_ticket_number,
         |    ss_quantity
         |  FROM
         |    store_sales ss,
         |    date_dim d1
         |  WHERE d1.d_year = ${q21_year}
         |  AND d1.d_moy = ${Paras.q21_month}
         |  AND ss.ss_sold_date_sk = d1.d_date_sk
         |) part_ss ON (
         |  part_ss.ss_ticket_number = part_sr.sr_ticket_number
         |  AND part_ss.ss_item_sk = part_sr.sr_item_sk
         |  AND part_ss.ss_customer_sk = part_sr.sr_customer_sk
         |)
         |INNER JOIN store part_s ON (
         |  part_s.s_store_sk = part_ss.ss_store_sk
         |)
         |INNER JOIN item part_i ON (
         |  part_i.i_item_sk = part_ss.ss_item_sk
         |)
         |GROUP BY
         |  part_i.i_item_id,
         |  part_i.i_item_desc,
         |  part_s.s_store_id,
         |  part_s.s_store_name
         |ORDER BY
         |  part_i.i_item_id,
         |  part_i.i_item_desc,
         |  part_s.s_store_id,
         |  part_s.s_store_name
         |LIMIT ${q21_limit}
       """.stripMargin).collect()
  }

  val run_q22 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(22, vid)
    val q22_date = Paras.q22_date_list(i1)
    val q22_i_current_price_min = Paras.q22_i_current_price_min_list(i2)

    println(s"The default paramters for template 22 are: q22_date = ${Paras.q22_date_list(0)}, " +
      s"q22_i_current_price_min = ${Paras.q22_i_current_price_min_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q22_date = $q22_date, q22_i_current_price_min = $q22_i_current_price_min")
    }

    spark.sql(
      s"""
         |SELECT
         |  w_warehouse_name,
         |  i_item_id,
         |  SUM( CASE WHEN datediff(d_date, '${q22_date}') < 0
         |    THEN inv_quantity_on_hand
         |    ELSE 0 END
         |  ) AS inv_before,
         |  SUM( CASE WHEN datediff(d_date, '${q22_date}') >= 0
         |    THEN inv_quantity_on_hand
         |    ELSE 0 END
         |  ) AS inv_after
         |FROM inventory inv,
         |  item i,
         |  warehouse w,
         |  date_dim d
         |WHERE i_current_price BETWEEN ${q22_i_current_price_min} AND ${Paras.q22_i_current_price_max}
         |AND i_item_sk        = inv_item_sk
         |AND inv_warehouse_sk = w_warehouse_sk
         |AND inv_date_sk      = d_date_sk
         |AND datediff(d_date, '${q22_date}') >= -30
         |AND datediff(d_date, '${q22_date}') <= 30
         |GROUP BY w_warehouse_name, i_item_id
         |HAVING inv_before > 0
         |AND inv_after / inv_before >= 2.0 / 3.0
         |AND inv_after / inv_before <= 3.0 / 2.0
         |ORDER BY w_warehouse_name, i_item_id
         |LIMIT 100
       """.stripMargin).collect()
  }

  val run_q23 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(23, vid)
    val q23_year = Paras.q23_year_list(i1)
    val q23_month = Paras.q23_month_list(i2)

    println(s"The default paramters for template 23 are: q23_year = ${Paras.q23_year_list(0)}, " +
      s"q23_month = ${Paras.q23_month_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q23_year = $q23_year, q23_month = $q23_month")
    }


    val tmp_tbl = "tmp_tbl"

    val tmp_df = spark.sql(
      s"""
         |SELECT
         |  inv_warehouse_sk,
         | -- w_warehouse_name,
         |  inv_item_sk,
         |  d_moy,
         |  cast( ( stdev / mean ) as decimal(15,5)) cov
         |FROM (
         |   --Iteration 1: Calculate the coefficient of variation and mean of every item
         |   -- and warehouse of the given and the consecutive month
         |  SELECT
         |    inv_warehouse_sk,
         |    inv_item_sk,
         |    d_moy,
         |    -- implicit group by d_moy using CASE filters inside the stddev_samp() and avg() UDF's. This saves us from requiring a self join for correlation of d_moy and d_moy+1 later on.
         |    cast( stddev_samp( inv_quantity_on_hand ) as decimal(15,5)) stdev,
         |    cast(         avg( inv_quantity_on_hand ) as decimal(15,5)) mean
         |
         |  FROM inventory inv
         |  JOIN date_dim d
         |       ON (inv.inv_date_sk = d.d_date_sk
         |       AND d.d_year = ${q23_year}
         |       AND d_moy between ${q23_month} AND (${q23_month} + 1)
         |       )
         |  GROUP BY
         |    inv_warehouse_sk,
         |    inv_item_sk,
         |    d_moy
         |) q23_tmp_inv_part
         |--JOIN warehouse w ON inv_warehouse_sk = w.w_warehouse_sk
         |WHERE mean > 0 --avoid "div by 0"
         |  AND stdev/mean >= ${Paras.q23_coefficient}
       """.stripMargin)

    tmp_df.createOrReplaceTempView(tmp_tbl)

    spark.sql(
      s"""
         |SELECT
         |  inv1.inv_warehouse_sk,
         |  inv1.inv_item_sk,
         |  inv1.d_moy,
         |  inv1.cov,
         |  inv2.d_moy,
         |  inv2.cov
         |FROM ${tmp_tbl} inv1
         |JOIN ${tmp_tbl} inv2
         |    ON(   inv1.inv_warehouse_sk=inv2.inv_warehouse_sk
         |      AND inv1.inv_item_sk =  inv2.inv_item_sk
         |      AND inv1.d_moy = ${q23_month}
         |      AND inv2.d_moy = ${q23_month} + 1
         |    )
         |ORDER BY
         | inv1.inv_warehouse_sk,
         | inv1.inv_item_sk
       """.stripMargin).collect()

    // clean
    spark.catalog.dropTempView(tmp_tbl)

  }

  val run_q24 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(24, vid)
    val q24_i_item_sk = Paras.q24_i_item_sk_list(i1)
    val q24_limit = Paras.q24_limit_list(i2)

    println(s"The default paramters for template 24 are: q24_i_item_sk = ${Paras.q24_i_item_sk_list(0)}, " +
      s"q24_limit by default = None")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q24_i_item_sk = $q24_i_item_sk, q24_limit = $q24_limit")
    }

    val tmp_tbl = "tmp_tbl"

    val tmp_df = spark.sql(
      s"""
         |SELECT
         |  i_item_sk,
         |  imp_sk,
         |  --imp_competitor,
         |  (imp_competitor_price - i_current_price)/i_current_price AS price_change,
         |  imp_start_date,
         |  (imp_end_date - imp_start_date) AS no_days_comp_price
         |FROM item i ,item_marketprices imp
         |WHERE i.i_item_sk = imp.imp_item_sk
         |AND i.i_item_sk = ${q24_i_item_sk}
         |-- AND imp.imp_competitor_price < i.i_current_price --consider all price changes not just where competitor is cheaper
         |ORDER BY i_item_sk,
         |         imp_sk,
         |         --imp_competitor, --add to compute cross_price_elasticity per competitor is instead of a single number
         |         imp_start_date
         |${if (i2 > 0) s"LIMIT $q24_limit" else ""}
       """.stripMargin)
    tmp_df.createOrReplaceTempView(tmp_tbl)

    spark.sql(
      s"""
         |SELECT ws_item_sk,
         |       --ws.imp_competitor, --add to compute cross_price_elasticity per competitor is instead of a single number
         |       avg ( (current_ss_quant + current_ws_quant - prev_ss_quant - prev_ws_quant) / ((prev_ss_quant + prev_ws_quant) * ws.price_change)) AS cross_price_elasticity
         |FROM
         |    ( --websales items sold quantity before and after competitor price change
         |      SELECT
         |        ws_item_sk,
         |        imp_sk,
         |        --imp_competitor, --add to compute cross_price_elasticity per competitor is instead of a single number
         |        price_change,
         |        SUM( CASE WHEN  ( (ws_sold_date_sk >= c.imp_start_date) AND (ws_sold_date_sk < (c.imp_start_date + c.no_days_comp_price))) THEN ws_quantity ELSE 0 END ) AS current_ws_quant,
         |        SUM( CASE WHEN  ( (ws_sold_date_sk >= (c.imp_start_date - c.no_days_comp_price)) AND (ws_sold_date_sk < c.imp_start_date)) THEN ws_quantity ELSE 0 END ) AS prev_ws_quant
         |      FROM web_sales ws
         |      JOIN ${tmp_tbl} c ON ws.ws_item_sk = c.i_item_sk
         |      GROUP BY ws_item_sk,
         |              imp_sk,
         |              --imp_competitor,
         |              price_change
         |    ) ws
         |JOIN
         |    (--storesales items sold quantity before and after competitor price change
         |      SELECT
         |        ss_item_sk,
         |        imp_sk,
         |        --imp_competitor, --add to compute cross_price_elasticity per competitor is instead of a single number
         |        price_change,
         |        SUM( CASE WHEN ((ss_sold_date_sk >= c.imp_start_date) AND (ss_sold_date_sk < (c.imp_start_date + c.no_days_comp_price))) THEN ss_quantity ELSE 0 END) AS current_ss_quant,
         |        SUM( CASE WHEN ((ss_sold_date_sk >= (c.imp_start_date - c.no_days_comp_price)) AND (ss_sold_date_sk < c.imp_start_date)) THEN ss_quantity ELSE 0 END) AS prev_ss_quant
         |      FROM store_sales ss
         |      JOIN ${tmp_tbl} c ON c.i_item_sk = ss.ss_item_sk
         |      GROUP BY ss_item_sk,
         |              imp_sk,
         |              --imp_competitor, --add to compute cross_price_elasticity per competitor is instead of a single number
         |              price_change
         |    ) ss
         | ON (ws.ws_item_sk = ss.ss_item_sk and ws.imp_sk = ss.imp_sk)
         |GROUP BY  ws.ws_item_sk
       """.stripMargin).collect()

    // clean
    spark.catalog.dropTempView(tmp_tbl)

  }

  val run_q25 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(25, vid)
    val q25_date = Paras.q25_date_list(i1)
    val q25_numcluster : String= Paras.q25_numcluster_list(i2)

    println(s"The default paramters for template 25 are: q25_date = ${Paras.q25_date_list(0)}, " +
      s"q25_numcluster = ${Paras.q25_numcluster_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q25_date = $q25_date, q25_numcluster = $q25_numcluster")
    }

    // step 1: extract the input data
    val tmp_tbl = "tmp_tbl"

    val tmp_df1 = spark.sql(
      s"""
         |SELECT
         |  ss_customer_sk                     AS cid,
         |  count(distinct ss_ticket_number)   AS frequency,
         |  max(ss_sold_date_sk)               AS most_recent_date,
         |  SUM(ss_net_paid)                   AS amount
         |FROM store_sales ss
         |JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
         |WHERE d.d_date > '${q25_date}'
         |AND ss_customer_sk IS NOT NULL
         |GROUP BY ss_customer_sk
       """.stripMargin)
    val tmp_df2 = spark.sql(
      s"""
         |SELECT
         |  ws_bill_customer_sk             AS cid,
         |  count(distinct ws_order_number) AS frequency,
         |  max(ws_sold_date_sk)            AS most_recent_date,
         |  SUM(ws_net_paid)                AS amount
         |FROM web_sales ws
         |JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
         |WHERE d.d_date > '${q25_date}'
         |AND ws_bill_customer_sk IS NOT NULL
         |GROUP BY ws_bill_customer_sk
       """.stripMargin)

    val tmp_df = tmp_df1.union(tmp_df2)
    tmp_df.createOrReplaceTempView(tmp_tbl)

    val data = spark.sql(
      s"""
         |SELECT
         |  -- rounding of values not necessary
         |  cid            AS cid,
         |  CASE WHEN 37621 - max(most_recent_date) < 60 THEN 1.0 ELSE 0.0 END
         |                 AS recency, -- 37621 == 2003-01-02
         |  SUM(frequency) AS frequency, --total frequency
         |  SUM(amount)    AS totalspend --total amount
         |FROM ${tmp_tbl}
         |GROUP BY cid
         |--CLUSTER BY cid --cluster by preceded by group by is silently ignored by hive but fails in spark
         |--no total ordering with ORDER BY required, further processed by clustering algorithm
         |ORDER BY cid
       """.stripMargin)

    data.cache()

    // step 2: Calculating KMeans with spark (DataFrame)
    var options = Map(
      'iter -> "20"
    )
    import spark.implicits._

    // vectorize all columns except the first column
    // (a, b, c, d, e) => (a, [b, c, d, e])
    val assembler = new VectorAssembler()
      .setInputCols(data.columns.tail)
      .setOutputCol("features")

    val kmeans = new KMeans()
      .setFeaturesCol(assembler.getOutputCol)
      .setK(q25_numcluster.toInt)
      .setMaxIter(options('iter).toInt)
      .setInitMode("k-means||")
      .setSeed(1234567890)

    val pipeline = new Pipeline()
      .setStages(Array(assembler, kmeans))

    val model = pipeline.fit(data)

    // result of the transformation and clustering
    val predictions = model.transform(data)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)

    // get the KMeansModel for meta information
    // Evaluate clusterModel by computing Within Set Sum of Squared Errors
    val kMeansModel = model.stages(1).asInstanceOf[KMeansModel]
    val clusterCenters = kMeansModel.clusterCenters

    //print and write clustering  metadata
    val metaInformation =
      s"""Clusters:
         |
         |Number of Clusters: ${clusterCenters.length}
         |silhouette: $silhouette
         |${clusterCenters.mkString("\n")}
       """.stripMargin
    println(metaInformation)

    // step 3: clean
    data.unpersist()
    spark.catalog.dropTempView(tmp_tbl)

  }

  val run_q26 = (spark: SparkSession, vid: Int) => {

    // [reversed] i1 x i2 = 5 x 8
    val (i1, i2) = parse_vid(26, vid)
    val q26_i_category_IN = Paras.q26_i_category_IN_list(i1)
    val q26_numcluster : String = Paras.q26_numcluster_list(i2)

    println(s"The default paramters for template 26 are: q26_i_category_IN = ${Paras.q26_i_category_IN_list(0)}, " +
      s"q26_numcluster = ${Paras.q26_numcluster_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q26_i_category_IN = $q26_i_category_IN, q26_numcluster = $q26_numcluster")
    }

    // step 1: extract the input data
    val data = spark.sql(
      s"""
         |SELECT
         |  ss.ss_customer_sk AS cid,
         |  count(CASE WHEN i.i_class_id=1  THEN 1 ELSE NULL END) AS id1,
         |  count(CASE WHEN i.i_class_id=2  THEN 1 ELSE NULL END) AS id2,
         |  count(CASE WHEN i.i_class_id=3  THEN 1 ELSE NULL END) AS id3,
         |  count(CASE WHEN i.i_class_id=4  THEN 1 ELSE NULL END) AS id4,
         |  count(CASE WHEN i.i_class_id=5  THEN 1 ELSE NULL END) AS id5,
         |  count(CASE WHEN i.i_class_id=6  THEN 1 ELSE NULL END) AS id6,
         |  count(CASE WHEN i.i_class_id=7  THEN 1 ELSE NULL END) AS id7,
         |  count(CASE WHEN i.i_class_id=8  THEN 1 ELSE NULL END) AS id8,
         |  count(CASE WHEN i.i_class_id=9  THEN 1 ELSE NULL END) AS id9,
         |  count(CASE WHEN i.i_class_id=10 THEN 1 ELSE NULL END) AS id10,
         |  count(CASE WHEN i.i_class_id=11 THEN 1 ELSE NULL END) AS id11,
         |  count(CASE WHEN i.i_class_id=12 THEN 1 ELSE NULL END) AS id12,
         |  count(CASE WHEN i.i_class_id=13 THEN 1 ELSE NULL END) AS id13,
         |  count(CASE WHEN i.i_class_id=14 THEN 1 ELSE NULL END) AS id14,
         |  count(CASE WHEN i.i_class_id=15 THEN 1 ELSE NULL END) AS id15
         |FROM store_sales ss
         |INNER JOIN item i
         |  ON (ss.ss_item_sk = i.i_item_sk
         |  AND i.i_category IN (${q26_i_category_IN})
         |  AND ss.ss_customer_sk IS NOT NULL
         |)
         |GROUP BY ss.ss_customer_sk
         |HAVING count(ss.ss_item_sk) > ${Paras.q26_count_ss_item_sk}
         |--CLUSTER BY cid --cluster by preceded by group by is silently ignored by hive but fails in spark
         |ORDER BY cid
       """.stripMargin)

    data.cache()

    // step 2: Calculating KMeans with spark (DataFrame)
    var options = Map(
      'iter -> "20"
    )
    import spark.implicits._

    // vectorize all columns except the first column
    // (a, b, c, d, e) => (a, [b, c, d, e])
    val assembler = new VectorAssembler()
      .setInputCols(data.columns.tail)
      .setOutputCol("features")

    val kmeans = new KMeans()
      .setFeaturesCol(assembler.getOutputCol)
      .setK(q26_numcluster.toInt)
      .setMaxIter(options('iter).toInt)
      .setInitMode("k-means||")
      .setSeed(1234567890)

    val pipeline = new Pipeline()
      .setStages(Array(assembler, kmeans))

    val model = pipeline.fit(data)

    // result of the transformation and clustering
    val predictions = model.transform(data)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()

    val silhouette = evaluator.evaluate(predictions)

    // get the KMeansModel for meta information
    val kMeansModel = model.stages(1).asInstanceOf[KMeansModel]

    // Evaluate clusterModel by computing Within Set Sum of Squared Errors
    val clusterCenters = kMeansModel.clusterCenters

    //print and write clustering  metadata
    val metaInformation =
      s"""Clusters:
         |
         |Number of Clusters: ${clusterCenters.length}
         |silhouette: $silhouette
         |${clusterCenters.mkString("\n")}
       """.stripMargin
    println(metaInformation)

    // clean
    data.unpersist()
  }

  val run_q27 = (spark: SparkSession, vid: Int) => {

    // [reversed] i1 x i2 = 5 x 8
    val (i1, i2) = parse_vid(27, vid)
    val q27_pr_item_sk = Paras.q27_pr_item_sk_list(i1)
    val q27_limit = Paras.q27_limit_list(i2)

    println(s"The default paramters for template 27 are: q27_pr_item_sk = ${Paras.q27_pr_item_sk_list(0)}, " +
      s"q27_limit by default = None")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q27_pr_item_sk = $q27_pr_item_sk, q27_limit = $q27_limit")
    }

    spark.sql("CREATE TEMPORARY FUNCTION find_company AS 'io.bigdatabenchmark.v1.queries.q27.CompanyUDF'")
    spark.sql(
      s"""
         |SELECT review_sk, item_sk, company_name, review_sentence
         |FROM ( --wrap in additional FROM(), because sorting/distribute by with UDTF in select clause is not allowed
         |  -- find_company() searches for competitor products in the reviews of q27_pr_item_sk
         |  SELECT find_company(pr_review_sk, pr_item_sk, pr_review_content) AS (review_sk, item_sk, company_name, review_sentence)
         |  FROM (
         |    SELECT pr_review_sk, pr_item_sk, pr_review_content
         |    FROM product_reviews
         |    WHERE pr_item_sk = ${q27_pr_item_sk}
         |  ) subtable
         |) extracted
         |ORDER BY review_sk, item_sk, company_name, review_sentence
         |${if (i2 > 0) s"LIMIT $q27_limit" else ""}
       """.stripMargin).collect()

    // clean
    spark.sql("DROP TEMPORARY FUNCTION find_company")
  }

  val run_q28 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(28, vid)
    val q28_lambda : String = Paras.q28_lambda_list(i1)
    val q28_additional_time_pressure_rate = Paras.q28_additional_time_pressure_rate_list(i2)

    println(s"The default paramters for template 28 are: q28_lambda by default is = ${Paras.q28_lambda_list(0)}, " +
      s"q28_additional_time_pressure_rate by default = ${Paras.q28_additional_time_pressure_rate_list(0)}")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q28_lambda = $q28_lambda, " +
        s"q28_additional_time_pressure_rate = $q28_additional_time_pressure_rate")
    }

    // step 1: extract the input data
    val inputTrain = spark.sql(s"""
         |SELECT pr_review_sk AS pr_review_sk, pr_review_rating AS pr_rating, pr_review_content AS pr_review_content
         |FROM (
         |  SELECT
         |    pr_review_sk,
         |    pr_review_rating,
         |    pr_review_content
         |  FROM product_reviews
         |  ORDER BY pr_review_sk
         |)
         |WHERE pmod(pr_review_sk, 10) IN (1,2,3,4,5,6,7,8,9) -- 90% are training
         |""".stripMargin)
    val inputTest = spark.sql(
      s"""
         |SELECT pr_review_sk AS pr_review_sk, pr_review_rating AS pr_rating, pr_review_content AS pr_review_content
         |FROM (
         |  SELECT
         |    pr_review_sk,
         |    pr_review_rating,
         |    pr_review_content
         |  FROM product_reviews
         |  ORDER BY pr_review_sk
         |)
         |WHERE pmod(pr_review_sk, 10) IN (0) -- 10% are testing
         |""".stripMargin)

    // step 2: Train and Test Naive Bayes Classifier with spark Dataframe

//    var options = Map('lambda -> "0")
    import spark.implicits._
    def ratingToDoubleLabel(label: Int): Double = {
      label match {
        case 1 => 0.0
        case 2 => 0.0
        case 3 => 1.0
        case 4 => 2.0
        case 5 => 2.0
        case _ => 1.0
      }
    }

    val transformRating = udf((r : Int) => ratingToDoubleLabel(r))
    val trainingData = inputTrain.withColumn("sentiment", transformRating($"pr_rating")).cache()
    val testingData = inputTest.withColumn("sentiment", transformRating($"pr_rating")).cache()

    // set up pipeline
    val tokenizer = new Tokenizer()
      .setInputCol("pr_review_content")
      .setOutputCol("words")

    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("rawFeature")

    val idf = new IDF()
      .setInputCol(hashingTF.getOutputCol)
      .setOutputCol("feature")

    val naiveBayes = new NaiveBayes()
      .setSmoothing(q28_lambda.toDouble)
      .setFeaturesCol(idf.getOutputCol)
      .setLabelCol("sentiment")
      .setModelType("multinomial")

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, idf, naiveBayes))

    // ---> testing time pressure
    val training_start_time : Long = System.currentTimeMillis

    // train the model (Tokenize -> TF/IDF -> Naive Bayes)
    val model = pipeline.fit(trainingData)

    // ---> testing time pressure
    val training_end_time : Long = System.currentTimeMillis

    // TODO ---> add different time pressure here

    def factorial(n:BigInt):BigInt = if (n==0) 1 else n * factorial(n-1)

    val training_duration = training_end_time - training_start_time
    println(s"---> trianing time: $training_duration ms")
    val additional_time_pressure = (training_duration * q28_additional_time_pressure_rate).toLong
    val additional_pressure_start : Long = System.currentTimeMillis
    while(System.currentTimeMillis - additional_pressure_start < additional_time_pressure) {
      factorial(100)
    }

    println("Testing NaiveBayes model")
    // get the predictions
    val prediction : DataFrame = model.transform(testingData).cache()

    // calculate metrics
    val predictionAndLabel = prediction
      .select($"prediction", $"sentiment")
      .rdd.map({case Row(prediction: Double, sentiment: Double) => prediction -> sentiment})
    val multMetrics = new MulticlassMetrics(predictionAndLabel)

    val prec = multMetrics.weightedPrecision
    val acc = multMetrics.accuracy
    val confMat = multMetrics.confusionMatrix

    //calculate Metadata about created model
    val metaInformation=
      s"""Precision: $prec
         |Accuracy: $acc
         |Confusion Matrix:
         |$confMat
         |""".stripMargin

    println(metaInformation)

    // clean
    inputTrain.unpersist()
    inputTest.unpersist()
    prediction.unpersist()

  }

  val run_q29 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(29, vid)
    val q29_limit = Paras.q29_limit_list(i1)
    val q29_ws_quantity_upper = Paras.q29_ws_quantity_upper_list(i2)

    println(s"The default paramters for template 29 are: q29_limit = ${Paras.q29_limit_list(0)}, " +
      s"q29_ws_quantity_upper by default = None")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q29_limit = $q29_limit, q29_ws_quantity_upper = $q29_ws_quantity_upper")
    }

    spark.sql("CREATE TEMPORARY FUNCTION makePairs AS 'io.bigdatabenchmark.v1.queries.udf.PairwiseUDTF'")


    // TODO add a type filter for web_sales
    spark.sql(
      s"""
         |SELECT category_id_1, category_id_2, COUNT (*) AS cnt
         |FROM (
         |  -- Make category "purchased together" pairs
         |  -- combining collect_set + sorting + makepairs(array, noSelfParing)
         |  -- ensures we get no pairs with swapped places like: (12,24),(24,12).
         |  -- We only produce tuples (12,24) ensuring that the smaller number is always on the left side
         |  SELECT makePairs(sort_array(itemArray), false) AS (category_id_1,category_id_2)
         |  FROM (
         |    SELECT collect_set(i_category_id) as itemArray --(_list= with duplicates, _set = distinct)
         |    FROM web_sales ws, item i
         |    WHERE ws.ws_item_sk = i.i_item_sk
         |    ${if (i2 > 0) s"AND ws.ws_quantity < ${q29_ws_quantity_upper}" else ""}
         |    AND i.i_category_id IS NOT NULL
         |    GROUP BY ws_order_number
         |  ) collectedList
         |) pairs
         |GROUP BY category_id_1, category_id_2
         |ORDER BY cnt DESC, category_id_1, category_id_2
         |LIMIT ${q29_limit}
       """.stripMargin).collect()

    // clean
    spark.sql("DROP TEMPORARY FUNCTION makePairs")

  }

  val run_q30 = (spark: SparkSession, vid: Int) => {

    // i1 x i2 = 8 x 5
    val (i1, i2) = parse_vid(30, vid)
    val q30_limit = Paras.q30_limit_list(i1)
    val q30_wcs_click_date_upper = Paras.q30_wcs_click_date_upper_list(i2)

    println(s"The default paramters for template 30 are: q30_limit = ${Paras.q30_limit_list(0)}, " +
      s"q30_wcs_click_date_upper by default = None")
    if (vid == 0) {
      println(s"---> Current: IS default parameters!")
    } else {
      println(s"---> Current: q30_limit = $q30_limit, q30_wcs_click_date_upper = $q30_wcs_click_date_upper")
    }

    spark.sql("CREATE TEMPORARY FUNCTION makePairs AS 'io.bigdatabenchmark.v1.queries.udf.PairwiseUDTF'")

    val tmp_tbl = "tmp_tbl"

    // TODO add a filter (timestamp) for web_clickstreams --> half half
    val tmp_df = spark.sql(
      s"""
         |SELECT *
         |FROM (
         |  FROM (
         |    SELECT wcs_user_sk,
         |      (wcs_click_date_sk*24L*60L*60L + wcs_click_time_sk) AS tstamp_inSec,
         |      i_category_id
         |    FROM web_clickstreams wcs, item i
         |    WHERE wcs.wcs_item_sk = i.i_item_sk
         |    ${if (i2 > 0) s"AND wcs.wcs_click_date_sk <= ${q30_wcs_click_date_upper}" else ""}
         |    AND i.i_category_id IS NOT NULL
         |    AND wcs.wcs_user_sk IS NOT NULL
         |    --Hive uses the columns in Distribute By to distribute the rows among reducers. All rows with the same Distribute By columns will go to the same reducer. However, Distribute By does not guarantee clustering or sorting properties on the distributed keys.
         |    DISTRIBUTE BY wcs_user_sk SORT BY wcs_user_sk, tstamp_inSec, i_category_id -- "sessionize" reducer script requires the cluster by uid and sort by tstamp ASCENDING
         |  ) clicksAnWebPageType
         |  REDUCE
         |    wcs_user_sk,
         |    tstamp_inSec,
         |    i_category_id
         |  USING 'python q30-sessionize.py ${Paras.q30_session_timeout_inSec}'
         |  AS (
         |    category_id BIGINT,
         |    sessionid STRING
         |  )
         |) q02_tmp_sessionize
         |Cluster by sessionid
       """.stripMargin)
    tmp_df.createOrReplaceTempView(tmp_tbl)

    spark.sql(
      s"""
         |SELECT  category_id_1, category_id_2, COUNT (*) AS cnt
         |FROM (
         |  -- Make category "viewed together" pairs
         |  -- combining collect_set + sorting + makepairs(array, noSelfParing)
         |  -- ensures we get no pairs with swapped places like: (12,24),(24,12).
         |  -- We only produce tuples (12,24) ensuring that the smaller number is always on the left side
         |  SELECT makePairs(sort_array(itemArray), false) AS (category_id_1,category_id_2)
         |  FROM (
         |    SELECT collect_set(category_id) as itemArray --(_list= with duplicates, _set = distinct)
         |    FROM ${tmp_tbl}
         |    GROUP BY sessionid
         |  ) collectedList
         |) pairs
         |GROUP BY category_id_1, category_id_2
         |ORDER BY cnt DESC, category_id_1, category_id_2
         |LIMIT ${q30_limit}
       """.stripMargin).collect()

    // clean
    spark.catalog.dropTempView(tmp_tbl)
    spark.sql("DROP TEMPORARY FUNCTION makePairs")
  }


}
