package chenghao.tpcxbb

import chenghao.tpcxbb.Paras._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegressionModel, NaiveBayes, LogisticRegression => LogisticRegressionSpark}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.PrintWriter


object Queries {

  def expose_logical_plan(spark: SparkSession, qid: Int, queryContent: String, header: String): Unit = {
    val logicPlan = spark.sql("explain cost " + queryContent).head().getString(0)
    new java.io.File(s"${header}/${qid}").mkdirs
    val filename = s"${header}/${qid}/${spark.sparkContext.appName}.txt"
    new PrintWriter(filename) { write(logicPlan); close }
  }

  val run_q1 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val q01_i_category_id_IN = q01_i_category_id_IN_list(vid - 1)
    spark.sql("CREATE TEMPORARY FUNCTION makePairs AS 'io.bigdatabenchmark.v1.queries.udf.PairwiseUDTF'")
    val queryContent = s"""
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
         |    AND s.ss_store_sk IN (${q01_ss_store_sk_IN})
         |    GROUP BY ss_ticket_number
         |  ) soldItemsPerTicket
         |) soldTogetherPairs
         |GROUP BY item_sk_1, item_sk_2
         |-- 'frequently'
         |HAVING cnt > ${q01_viewed_together_count}
         |ORDER BY cnt DESC, item_sk_1, item_sk_2
         |LIMIT ${q01_limit}
      """.stripMargin
    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 1, queryContent, header)
    //  clean
    spark.sql("DROP TEMPORARY FUNCTION makePairs")
  }

  val run_q2 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val q02_item_sk = q02_item_sk_list(vid - 1)
    spark.sql("CREATE TEMPORARY FUNCTION makePairs AS 'io.bigdatabenchmark.v1.queries.udf.PairwiseUDTF'")
    val tmp_tbl = "tmp_view"
    val queryContent =
      s"""
        |with ${tmp_tbl} as (
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
        |  USING 'python q2-sessionize.py ${q02_session_timeout_inSec}'
        |  AS (
        |    wcs_item_sk BIGINT,
        |    sessionid STRING)
        |) q02_tmp_sessionize
        |CLUSTER BY sessionid
        |)
        |
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
        |""".stripMargin
    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 2, queryContent, header)
    //  clean
    spark.sql("DROP TEMPORARY FUNCTION makePairs")
  }

  val run_q3 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val q03_purchased_item_IN = q03_purchased_item_IN_list(vid - 1)
    val queryContent =
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
         |    USING 'python q03_filterLast_N_viewedItmes_within_y_days.py ${q03_days_in_sec_before_purchase} ${q03_views_before_purchase} ${q03_purchased_item_IN}'
         |    AS (purchased_item BIGINT, lastviewed_item BIGINT)
         |  ) lastViewSessions
         |  WHERE i.i_item_sk = lastViewSessions.lastviewed_item
         |  AND i.i_category_id IN (${q03_purchased_item_category_IN}) --Only products in certain categories
         |  CLUSTER BY lastviewed_item,purchased_item -- pre-cluster to speed up following group by and count()
         |) distributed
         |GROUP BY purchased_item,lastviewed_item
         |ORDER BY cnt DESC, purchased_item, lastviewed_item
         |--DISTRIBUTE BY lastviewed_item SORT BY cnt DESC, purchased_item, lastviewed_item --cluster parallel sorting
         |LIMIT ${q03_limit}
      """.stripMargin
    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 3, queryContent, header)
  }

  val run_q4 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val q04_session_timeout_inSec = q04_session_timeout_inSec_list(vid - 1)

    val tmp_tbl = "tmp_tbl"
    val queryContent =
      s"""
        |with ${tmp_tbl} as (
        |FROM
        |(
        |  SELECT
        |    c.wcs_user_sk,
        |    w.wp_type,
        |    (wcs_click_date_sk * 24 * 60 * 60 + wcs_click_time_sk) AS tstamp_inSec
        |  FROM web_clickstreams c, web_page w
        |  WHERE c.wcs_web_page_sk = w.wp_web_page_sk
        |  AND   c.wcs_web_page_sk IS NOT NULL
        |  AND   c.wcs_user_sk     IS NOT NULL
        |  AND   c.wcs_sales_sk    IS NULL --abandoned implies: no sale
        |  DISTRIBUTE BY wcs_user_sk SORT BY wcs_user_sk, tstamp_inSec
        |) clicksAnWebPageType
        |REDUCE
        |  wcs_user_sk,
        |  tstamp_inSec,
        |  wp_type
        |USING 'python q4_sessionize.py ${q04_session_timeout_inSec}'
        |AS (
        |    wp_type STRING,
        |    tstamp BIGINT, --we require timestamp in further processing to keep output deterministic cross multiple reducers
        |    sessionid STRING
        |)
        |)
        |
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
        |""".stripMargin

    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 4, queryContent, header)
  }

  val run_q5 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {
    val (q05_i_category, q05_cd_gender, q05_cd_education_status_IN, q05_lambda) = q05_list(vid - 1)

    val queryContent =
      s"""
         |SELECT
         |  --wcs_user_sk,
         |  clicks_in_category,
         |  CASE WHEN cd_education_status IN (${q05_cd_education_status_IN}) THEN 1 ELSE 0 END AS college_education,
         |  CASE WHEN cd_gender = ${q05_cd_gender} THEN 1 ELSE 0 END AS male,
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
         |""".stripMargin
    if (!debug) {
      // step 1: extract the input data
      val rawData = spark.sql(queryContent)
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
    expose_logical_plan(spark, 5, queryContent, header)
  }

  val run_q6 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val q06_year = q06_year_list(vid - 1)
    val tmp_tbl1 = "tmp_tbl1"
    val tmp_tbl2 = "tmp_tbl2"
    val queryContent =
      s"""
        |with ${tmp_tbl1} as (
        |SELECT ss_customer_sk AS customer_sk,
        |       sum( case when (d_year = ${q06_year})   THEN (((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2)  ELSE 0 END) first_year_total,
        |       sum( case when (d_year = ${q06_year}+1) THEN (((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2)  ELSE 0 END) second_year_total
        |FROM  store_sales
        |     ,date_dim
        |WHERE ss_sold_date_sk = d_date_sk
        |AND   d_year BETWEEN ${q06_year} AND ${q06_year} +1
        |GROUP BY ss_customer_sk
        |HAVING first_year_total > 0
        |),
        |
        |${tmp_tbl2} as (
        |SELECT ws_bill_customer_sk AS customer_sk ,
        |       sum( case when (d_year = ${q06_year})   THEN (((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2)   ELSE 0 END) first_year_total,
        |       sum( case when (d_year = ${q06_year}+1) THEN (((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2)   ELSE 0 END) second_year_total
        |FROM web_sales
        |    ,date_dim
        |WHERE ws_sold_date_sk = d_date_sk
        |AND   d_year BETWEEN ${q06_year} AND ${q06_year} +1
        |GROUP BY ws_bill_customer_sk
        |HAVING first_year_total > 0
        |)
        |
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
        |""".stripMargin

    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 6, queryContent, header)
  }

  val run_q7 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val (q07_HIGHER_PRICE_RATIO, q07_year, q07_month) = q07_list(vid - 1)
    val tmp_tbl = "tmp_tbl"
    val queryContent =
      s"""
        |with ${tmp_tbl} as (
        |SELECT k.i_item_sk
        |FROM item k,
        |(
        |  SELECT
        |    i_category,
        |    AVG(j.i_current_price) * ${q07_HIGHER_PRICE_RATIO} AS avg_price
        |  FROM item j
        |  GROUP BY j.i_category
        |) avgCategoryPrice
        |WHERE avgCategoryPrice.i_category = k.i_category
        |AND k.i_current_price > avgCategoryPrice.avg_price
        |)
        |
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
        |  AND d_moy = ${q07_month}
        |)
        |GROUP BY ca_state
        |HAVING cnt >= ${q07_HAVING_COUNT_GE} --at least 10 customers
        |ORDER BY cnt DESC, ca_state --top 10 states in descending order
        |limit ${q07_LIMIT}
        |
        |""".stripMargin

    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 7, queryContent, header)
  }

  val run_q8 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val (q08_startDate, q08_endDate, q08_seconds_before_purchase) = q08_list(vid - 1)

    val tmp_tbl1 = "tmp_tbl1"
    val tmp_tbl2 = "tmp_tbl2"
    val tmp_tbl3 = "tmp_tbl3"
    val queryContent =
      s"""
        |with ${tmp_tbl1} as (
        |SELECT d_date_sk
        |FROM date_dim d
        |WHERE d.d_date >= '${q08_startDate}'
        |AND   d.d_date <= '${q08_endDate}'
        |),
        |
        |${tmp_tbl2} as (
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
        |)
        |,
        |${tmp_tbl3} as (
        |SELECT ws_net_paid, ws_order_number
        |FROM web_sales ws
        |JOIN ${tmp_tbl1} d ON ( ws.ws_sold_date_sk = d.d_date_sk)
        |)
        |
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
        |
        |""".stripMargin
    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 8, queryContent, header)
  }

  val run_q9 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {
    val (q09_year, q09_marital_stauts) = q09_list(vid - 1)

    val queryContent =
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
         |    cd.cd_marital_status = '${q09_marital_stauts}'
         |    AND cd.cd_education_status = '${q09_part1_education_status}'
         |    AND ${q09_part1_sales_price_min} <= ss1.ss_sales_price
         |    AND ss1.ss_sales_price <= ${q09_part1_sales_price_max}
         |  )
         |  OR
         |  (
         |    cd.cd_marital_status = '${q09_marital_stauts}'
         |    AND cd.cd_education_status = '${q09_part2_education_status}'
         |    AND ${q09_part2_sales_price_min} <= ss1.ss_sales_price
         |    AND ss1.ss_sales_price <= ${q09_part2_sales_price_max}
         |  )
         |  OR
         |  (
         |    cd.cd_marital_status = '${q09_marital_stauts}'
         |    AND cd.cd_education_status = '${q09_part3_education_status}'
         |    AND ${q09_part3_sales_price_min} <= ss1.ss_sales_price
         |    AND ss1.ss_sales_price <= ${q09_part3_sales_price_max}
         |  )
         |)
         |AND
         |(
         |  (
         |    ca1.ca_country = '${q09_part1_ca_country}'
         |    AND ca1.ca_state IN (${q09_part1_ca_state_IN})
         |    AND ${q09_part1_net_profit_min} <= ss1.ss_net_profit
         |    AND ss1.ss_net_profit <= ${q09_part1_net_profit_max}
         |  )
         |  OR
         |  (
         |    ca1.ca_country = '${q09_part2_ca_country}'
         |    AND ca1.ca_state IN (${q09_part2_ca_state_IN})
         |    AND ${q09_part2_net_profit_min} <= ss1.ss_net_profit
         |    AND ss1.ss_net_profit <= ${q09_part2_net_profit_max}
         |  )
         |  OR
         |  (
         |    ca1.ca_country = '${q09_part3_ca_country}'
         |    AND ca1.ca_state IN (${q09_part3_ca_state_IN})
         |    AND ${q09_part3_net_profit_min} <= ss1.ss_net_profit
         |    AND ss1.ss_net_profit <= ${q09_part3_net_profit_max}
         |  )
         |)
       """.stripMargin
    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 9, queryContent, header)

  }

  val run_q10 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val (q10_pr_review_rating_IN, q10_startDate) = q10_list(vid - 1)
    spark.sql("CREATE TEMPORARY FUNCTION extract_sentiment AS 'io.bigdatabenchmark.v1.queries.q10.SentimentUDF'")
    // ADD LIMIT 1000, 3000, 10000
    val queryContent =
      s"""
         |SELECT item_sk, review_sentence, sentiment, sentiment_word
         |FROM (--wrap in additional FROM(), because Sorting/distribute by with UDTF in select clause is not allowed
         |  SELECT extract_sentiment(pr_item_sk, pr_review_content) AS (item_sk, review_sentence, sentiment, sentiment_word)
         |  FROM product_reviews
         |  -- add by chenghao
         |  WHERE pr_review_rating in (${q10_pr_review_rating_IN})
         |  AND pr_review_date >= ${q10_startDate}
         |) extracted
         |ORDER BY item_sk,review_sentence,sentiment,sentiment_word
         |""".stripMargin

    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 10, queryContent, header)
    spark.sql("DROP TEMPORARY FUNCTION extract_sentiment")
  }

  val run_q11 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {
    val (q11_startDate, q11_endDate) = q11_list(vid - 1)
    val queryContent =
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
         |""".stripMargin
    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 11, queryContent, header)
  }

  val run_q12 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val q12_i_category_IN = q12_i_category_IN_list(vid - 1)
    val queryContent =
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
       """.stripMargin

    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 12, queryContent, header)
  }

  val run_q13 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val q13_year = q13_year_list(vid - 1)

    val tmp_tbl1 = "tmp_tbl1"
    val tmp_tbl2 = "tmp_tbl2"
    val queryContent =
      s"""
         |with ${tmp_tbl1} as (
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
         |)
         |,
         |${tmp_tbl2} as (
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
         |)
         |
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
         |
         |""".stripMargin
    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 13, queryContent, header)
  }

  val run_q14 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val (q14_dependents, q14_morning_startHour, q14_morning_endHour, q14_evening_startHour, q14_evening_endHour,
      q14_content_len_min, q14_content_len_max) = q14_list(vid - 1)
    val queryContent =
      s"""
         |SELECT CAST(amc as double) / CAST(pmc as double) am_pm_ratio
         |FROM (
         |  SELECT COUNT(*) amc
         |  FROM web_sales ws
         |  JOIN household_demographics hd ON hd.hd_demo_sk = ws.ws_ship_hdemo_sk
         |  AND hd.hd_dep_count = ${q14_dependents}
         |  JOIN time_dim td ON td.t_time_sk = ws.ws_sold_time_sk
         |  AND td.t_hour >= ${q14_morning_startHour}
         |  AND td.t_hour <= ${q14_morning_endHour}
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
         |  AND td.t_hour >= ${q14_evening_startHour}
         |  AND td.t_hour <= ${q14_evening_endHour}
         |  JOIN web_page wp ON wp.wp_web_page_sk = ws.ws_web_page_sk
         |  AND wp.wp_char_count >= ${q14_content_len_min}
         |  AND wp.wp_char_count <= ${q14_content_len_max}
         |) pt
       """.stripMargin
    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 14, queryContent, header)
  }

  val run_q15 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val (q15_startDate, q15_endDate, q15_store_sk) = q15_list(vid - 1)
    val queryContent =
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
       """.stripMargin
    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 15, queryContent, header)
  }

  val run_q16 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val q16_date = q16_date_list(vid - 1)
    val queryContent =
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
         |LIMIT 100
       """.stripMargin
    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 16, queryContent, header)
  }

  val run_q17 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val (q17_year, q17_month, q17_i_category_IN) = q17_list(vid - 1)
    val queryContent =
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
         |  WHERE ca_gmt_offset = ${q17_gmt_offset}
         |  AND s_gmt_offset = ${q17_gmt_offset}
         |  AND i_category IN (${q17_i_category_IN})
         |  AND d_year = ${q17_year}
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
         |  WHERE ca_gmt_offset = ${q17_gmt_offset}
         |  AND s_gmt_offset = ${q17_gmt_offset}
         |  AND i_category IN (${q17_i_category_IN})
         |  AND d_year = ${q17_year}
         |  AND d_moy = ${q17_month}
         |) all_sales
         |-- we don't need a 'ON' join condition. result is just two numbers.
         |ORDER BY promotions, total
         |LIMIT 100
       """.stripMargin

    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 17, queryContent, header)

  }

  val run_q18 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val (q18_startDate, q18_endDate) = q18_list(vid - 1)
    spark.sql("CREATE TEMPORARY FUNCTION extract_NegSentiment AS 'io.bigdatabenchmark.v1.queries.q18.NegativeSentimentUDF'")
    val tmp_tbl1 = "tmp_tbl1"
    val tmp_tbl2 = "tmp_tbl2"
    val queryContent =
      s"""
         |with ${tmp_tbl1} as (
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
         |)
         |,
         |${tmp_tbl2} as (
         |SELECT
         |  CONCAT(s_store_sk,"_", s_store_name ) AS store_ID, --this could be any string we want to identify the store. but as store_sk is just a number and store_name is ambiguous we choose the concatenation of both
         |  pr_review_date,
         |  pr_review_content
         |-- THIS IS A CROSS JOIN! but fortunately the "stores_with_regression" table is very small
         |FROM product_reviews pr, ${tmp_tbl1} stores_with_regression
         |WHERE locate(lower(stores_with_regression.s_store_name), lower(pr.pr_review_content), 1) >= 1 --find store name in reviews
         |)
         |
         |SELECT s_name, r_date, r_sentence, sentiment, sentiment_word
         |FROM ( --wrap in additional FROM(), because Sorting/distribute by with UDTF in select clause is not allowed
         |  --negative sentiment analysis of found reviews for stores with declining sales
         |  SELECT extract_NegSentiment(store_ID, pr_review_date, pr_review_content) AS ( s_name, r_date, r_sentence, sentiment, sentiment_word )
         |  FROM ${tmp_tbl2}
         |) extracted
         |ORDER BY s_name, r_date, r_sentence, sentiment,sentiment_word
         |""".stripMargin

    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 18, queryContent, header)
    // clean
    spark.sql("DROP TEMPORARY FUNCTION extract_NegSentiment")

  }

  val run_q19 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val q19_storeReturns_date_IN = q19_date_IN_list(vid - 1)
    val q19_webReturns_date_IN = q19_date_IN_list(vid - 1)

    spark.sql("CREATE TEMPORARY FUNCTION extract_sentiment AS 'io.bigdatabenchmark.v1.queries.q10.SentimentUDF'")
    val queryContent =
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
       """.stripMargin

    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 19, queryContent, header)
    // clean
    spark.sql("DROP TEMPORARY FUNCTION extract_sentiment")

  }

  val run_q20 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {
    val (q20_numclust, q20_iter) = q20_list(vid - 1)
    val queryContent =
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
        |""".stripMargin

    if (!debug) {
      // step 1: extract the input data
      val data = spark.sql(queryContent)
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
    expose_logical_plan(spark, 20, queryContent, header)

  }

  val run_q21 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {
    val (q21_year, q21_month) = q21_list(vid - 1)
    val queryContent =
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
         |	AND d2.d_moy BETWEEN ${q21_month} AND ${q21_month} + 6 --which were returned in the next six months
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
         |  AND d1.d_moy = ${q21_month}
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
       """.stripMargin
    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 21, queryContent, header)
  }

  val run_q22 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {
    val (q22_date, q22_i_current_price_max) = q22_list(vid - 1)
    val queryContent =
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
         |WHERE i_current_price BETWEEN ${q22_i_current_price_min} AND ${q22_i_current_price_max}
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
       """.stripMargin

    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 22, queryContent, header)
  }

  val run_q23 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {
    val (q23_year, q23_month, q23_coefficient) = q23_list(vid - 1)
    val tmp_tbl = "tmp_tbl"
    val queryContent =
      s"""
         |with ${tmp_tbl} as
         |(
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
         |  AND stdev/mean >= ${q23_coefficient}
         |)
         |
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
         |""".stripMargin
    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 23, queryContent, header)
  }

  val run_q24 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {
    val q24_i_item_sk = q24_i_item_sk_list(vid - 1)
    val tmp_tbl = "tmp_tbl"
    val queryContent =
      s"""
         |with $tmp_tbl as
         |(
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
         |)
         |
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
         |
         |""".stripMargin

    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 24, queryContent, header)

  }

  val run_q25 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {
    val (q25_date, q25_numcluster) = q25_list(vid - 1)

    // step 1: extract the input data
    val tmp_tbl = "tmp_tbl"
    val queryContent =
      s"""
         |with ${tmp_tbl} as (
         |
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
         |
         |union
         |
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
         |
         |)
         |
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
         |
         |""".stripMargin

    if (! debug) {
      val data = spark.sql(queryContent)
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
    expose_logical_plan(spark, 25, queryContent, header)
  }

  val run_q26 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {
    val (q26_i_category_IN, q26_numcluster) = q26_list(vid - 1)
    val queryContent =
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
         |HAVING count(ss.ss_item_sk) > ${q26_count_ss_item_sk}
         |--CLUSTER BY cid --cluster by preceded by group by is silently ignored by hive but fails in spark
         |ORDER BY cid
        |""".stripMargin

    if (!debug) {
      // step 1: extract the input data
      val data = spark.sql(queryContent)
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
    expose_logical_plan(spark, 26, queryContent, header)
  }

  val run_q27 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val q27_pr_item_sk = q27_pr_item_sk_list(vid - 1)

    spark.sql("CREATE TEMPORARY FUNCTION find_company AS 'io.bigdatabenchmark.v1.queries.q27.CompanyUDF'")
    val queryContent =
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
        |""".stripMargin
    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 27, queryContent, header)

    // clean
    spark.sql("DROP TEMPORARY FUNCTION find_company")
  }

  val run_q28 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {
    val (q28_lambda, q28_pr_review_rating_IN) = q28_list(vid - 1)
    val queryContent =
      s"""
         |SELECT pr_review_sk AS pr_review_sk, pr_review_rating AS pr_rating, pr_review_content AS pr_review_content
         |FROM (
         |  SELECT
         |    pr_review_sk,
         |    pr_review_rating,
         |    pr_review_content
         |  FROM product_reviews
         |  where pr_review_rating IN (${q28_pr_review_rating_IN})
         |  ORDER BY pr_review_sk
         |)
         |""".stripMargin

    if (! debug) {
      // step 1: extract the input data
      val inputTrain = spark.sql(
        s"""
          |$queryContent
          |WHERE pmod(pr_review_sk, 10) IN (1,2,3,4,5,6,7,8,9) -- 90% are training
          |""".stripMargin)
      val inputTest = spark.sql(
        s"""
           |$queryContent
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

      val transformRating = udf((r: Int) => ratingToDoubleLabel(r))
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

      // train the model (Tokenize -> TF/IDF -> Naive Bayes)
      val model = pipeline.fit(trainingData)

      println("Testing NaiveBayes model")
      // get the predictions
      val prediction: DataFrame = model.transform(testingData).cache()

      // calculate metrics
      val predictionAndLabel = prediction
        .select($"prediction", $"sentiment")
        .rdd.map({ case Row(prediction: Double, sentiment: Double) => prediction -> sentiment })
      val multMetrics = new MulticlassMetrics(predictionAndLabel)

      val prec = multMetrics.weightedPrecision
      val acc = multMetrics.accuracy
      val confMat = multMetrics.confusionMatrix

      //calculate Metadata about created model
      val metaInformation =
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
    expose_logical_plan(spark, 28, queryContent, header)
  }

  val run_q29 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {
    val q29_i_category_id_IN = q29_i_category_id_IN_list(vid - 1)
    spark.sql("CREATE TEMPORARY FUNCTION makePairs AS 'io.bigdatabenchmark.v1.queries.udf.PairwiseUDTF'")

    val queryContent =
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
        |    -- AND i.i_category_id IS NOT NULL
        |    AND i.i_category_id in (${q29_i_category_id_IN})
        |    GROUP BY ws_order_number
        |  ) collectedList
        |) pairs
        |GROUP BY category_id_1, category_id_2
        |ORDER BY cnt DESC, category_id_1, category_id_2
        |LIMIT ${q29_limit}
        |""".stripMargin

    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 29, queryContent, header)
    // clean
    spark.sql("DROP TEMPORARY FUNCTION makePairs")
  }

  val run_q30 = (spark: SparkSession, vid: Int, header: String, debug: Boolean) => {

    val q30_session_timeout_inSec = q30_session_timeout_inSec_list(vid - 1)
    spark.sql("CREATE TEMPORARY FUNCTION makePairs AS 'io.bigdatabenchmark.v1.queries.udf.PairwiseUDTF'")

    val tmp_tbl = "tmp_tbl"

    val queryContent =
      s"""
        |with ${tmp_tbl} as (
        |SELECT *
        |FROM (
        |  FROM (
        |    SELECT wcs_user_sk,
        |      (wcs_click_date_sk*24L*60L*60L + wcs_click_time_sk) AS tstamp_inSec,
        |      i_category_id
        |    FROM web_clickstreams wcs, item i
        |    WHERE wcs.wcs_item_sk = i.i_item_sk
        |    AND i.i_category_id IS NOT NULL
        |    AND wcs.wcs_user_sk IS NOT NULL
        |    --Hive uses the columns in Distribute By to distribute the rows among reducers. All rows with the same Distribute By columns will go to the same reducer. However, Distribute By does not guarantee clustering or sorting properties on the distributed keys.
        |    DISTRIBUTE BY wcs_user_sk SORT BY wcs_user_sk, tstamp_inSec, i_category_id -- "sessionize" reducer script requires the cluster by uid and sort by tstamp ASCENDING
        |  ) clicksAnWebPageType
        |  REDUCE
        |    wcs_user_sk,
        |    tstamp_inSec,
        |    i_category_id
        |  USING 'python q30-sessionize.py ${q30_session_timeout_inSec}'
        |  AS (
        |    category_id BIGINT,
        |    sessionid STRING
        |  )
        |) q02_tmp_sessionize
        |Cluster by sessionid
        |)
        |
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
        |""".stripMargin

    if (!debug)
      spark.sql(queryContent).collect()
    expose_logical_plan(spark, 30, queryContent, header)
    spark.sql("DROP TEMPORARY FUNCTION makePairs")
  }

}
