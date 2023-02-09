package chenghao.tpcxbb

object Paras {

  val i_category_ids = 1 to 9
  val pr_review_ratings = 1 to 5
  val i_categories = List("Home & Kitchen", "Toys & Games", "Sports & Outdoors", "Electronics", "Books",
    "Clothing & Accessories", "Music", "Tools & Home Improvement", "Movies & TV")

  // Q1
  //  val q01_i_category_id_IN="1, 2 ,3"
  val q01_i_category_id_IN_list = i_category_ids.combinations(2).map(_.mkString(",")).toList ++
    i_category_ids.combinations(3).map(_.mkString(",")).toList ++
    i_category_ids.combinations(4).map(_.mkString(",")).toList ++
    i_category_ids.combinations(5).map(_.mkString(",")).toList
  val q01_ss_store_sk_IN = "10, 20, 33, 40, 50"
  val q01_viewed_together_count = 50
  val q01_limit = 100

  //  Q2
  //  val q02_item_sk=10001
  val q02_item_sk_list = 10001 to 10101
  val q02_limit=30
//  val q02_limit_list = List(30, 15, 45, 20, 25, 35, 40, 50)
  val q02_session_timeout_inSec = 3600

  //  Q3
  val q03_days_in_sec_before_purchase = 864000
  val q03_views_before_purchase = 5
  //  val q03_purchased_item_IN=10001
  val q03_purchased_item_IN_list = 10001 to 10101
  val q03_purchased_item_category_IN = "2,3"
  val q03_limit=100
//  val q03_limit_list = List(100, 50, 150, 70, 80, 90, 110, 120)

  //  Q4
//  val q04_session_timeout_inSec = 3600
  val q04_session_timeout_inSec_list = 1800 to 7800 by 60

  //  Q5
  //  val q05_i_category="'Books'"
  //  Count(*) group by i_category. 'Books': 52870, 'Music': 81028, 'Movies & TV': 13369, others: 4k ~ 8k
  val q05_i_category_list = List("'Books'", "'Music'", "'Movies & TV'", "'Electronics'", "'Clothing & Accessories'")
  val q05_cd_gender_list = List("'M'", "'F'")
  val q05_cd_education_status_IN_list = List(
    "'Advanced Degree', 'College', '4 yr Degree', '2 yr Degree'",
    "'Advanced Degree', 'College', '4 yr Degree'"
  )
  val q05_lambda_list = List("0.0", "0.1", "0.2", "0.3", "0.4")
  val q05_list = for (
    i <- q05_i_category_list;
    j <- q05_cd_gender_list;
    k <- q05_cd_education_status_IN_list;
    g <- q05_lambda_list
  ) yield (i, j, k, g)

  //  Q6
  val q06_year_list = 1922 to 2022
  val q06_limit = 100

  //  Q7
  val q07_HIGHER_PRICE_RATIO_list = List(1.2, 1.4)
  //  val q07_YEAR=2004
  val q07_year_list = 2004 to 2008
  val q07_month_list = 1 to 10
  val q07_HAVING_COUNT_GE = 10
  val q07_LIMIT=10
  val q07_list = for (
    i <- q07_HIGHER_PRICE_RATIO_list;
    j <- q07_year_list;
    k <- q07_month_list
  ) yield (i, j, k)

  //  Q8
  val q08_startDate_list = List(
    "2001-03-02", "2001-06-02", "2001-09-02", "2001-12-02",
    "2002-03-02", "2002-06-02", "2002-09-02", "2002-12-02",
    "2003-03-02", "2003-06-02")
  val q08_year_gap = 1 to 2
  val q08_seconds_before_purchase_list = List(86400, 172800, 259200, 345600, 43200)
  val q08_list = for (
    i <- q08_startDate_list;
    j <- q08_year_gap;
    k <- q08_seconds_before_purchase_list
  ) yield (i, (i.split("-")(0).toInt + j).toString + i.slice(4, 10), k)


  //  Q9
  val q09_year_list = 1990 to 2010
  val q09_marital_status_list = List("M", "U", "D", "W", "S")
  val q09_list = for (
    i <- q09_year_list;
    j <- q09_marital_status_list
  ) yield (i, j)

  val q09_part1_ca_country = "United States"
  val q09_part1_ca_state_IN = "'KY', 'GA', 'NM'"
  val q09_part1_net_profit_min = 0
  val q09_part1_net_profit_max = 2000
  val q09_part1_education_status = "4 yr Degree"
  //  val q09_part1_marital_status="M"
  val q09_part1_sales_price_min = 100
  val q09_part1_sales_price_max = 150

  val q09_part2_ca_country = "United States"
  val q09_part2_ca_state_IN = "'MT', 'OR', 'IN'"
  val q09_part2_net_profit_min = 150
  val q09_part2_net_profit_max = 3000
  val q09_part2_education_status = "4 yr Degree"
  //  val q09_part2_marital_status="M"
  val q09_part2_sales_price_min = 50
  val q09_part2_sales_price_max = 200

  val q09_part3_ca_country = "United States"
  val q09_part3_ca_state_IN = "'WI', 'MO', 'WV'"
  val q09_part3_net_profit_min = 50
  val q09_part3_net_profit_max = 25000
  val q09_part3_education_status = "4 yr Degree"
  //  val q09_part3_marital_status="M"
  val q09_part3_sales_price_min = 150
  val q09_part3_sales_price_max = 200

  //  Q10
  // add by chenghao
  val q10_pr_review_rating_IN_list = pr_review_ratings.combinations(5).map(_.mkString(",")).toList ++
    pr_review_ratings.combinations(4).map(_.mkString(",")).toList ++
    pr_review_ratings.combinations(3).map(_.mkString(",")).toList ++
    pr_review_ratings.combinations(2).map(_.mkString(",")).toList
  val q10_startDate_list = List("2001-01-01", "2002-01-01", "2003-01-01", "2004-01-01", "2005-01-01")
  val q10_list = for (
    i <- q10_pr_review_rating_IN_list;
    j <- q10_startDate_list
  ) yield (i, j)

  //  Q11
  val q11_year_list = 2003 to 2008
  val q11_month_list = 1 to 10
  val q11_month_gap = 1 to 2
  val q11_list = for (
    y <- q11_year_list;
    m <- q11_month_list;
    g <- q11_month_gap
  ) yield (f"${y}-${m}%02d-02", f"${y}-${m + g}%02d-02")

  //  Q12
  val q12_i_category_IN_list = i_categories.combinations(3).map(_.map(x => s"'${x}'").mkString(",")).toList ++
    i_categories.combinations(2).map(_.map(x => s"'${x}'").mkString(",")).toList

  //  Q13
  val q13_year_list = 1922 to 2022
  val q13_limit = 100

  //  Q14
  val q14_dependents_list = 3 to 7 // 5 choices
  val q14_morning_startHour_list = 6 to 10 // 5 choices
  val q14_hour_gap = 1 to 2 // 2 choices
  val q14_content_len_min_list = List(4000, 5000)
  val q14_list = for (
    i <- q14_dependents_list;
    j <- q14_morning_startHour_list;
    k <- q14_hour_gap;
    m <- q14_content_len_min_list
  ) yield (i, j, j+k, j+12, j+12+k, m, m + 1000)


  //  Q15
  val q15_startDate_list = List("2003-01-02", "2003-02-02", "2003-03-02", "2003-04-02", "2003-05-02",
    "2003-06-02", "2003-07-02", "2003-08-02", "2003-09-02", "2003-10-02")
  val q15_store_sk_list = 1 to 10
  val q15_list = for (
    i <- q15_startDate_list;
    j <- q15_store_sk_list
  ) yield (i, (i.split("-")(0).toInt + 1).toString + i.slice(4, 10), j)

  //  Q16
  val q16_date_list = for(
    i <- 1 to 12;
    j <- 1 to 20
  ) yield (f"2001-${i}%02d-${j}%02d")

  //  Q17
  val q17_gmt_offset = "-5"
  val q17_year_list = 2001 to 2002
  val q17_month_list = 1 to 10
  val q17_i_category_IN_list = List("'Books', 'Music'", "'Books', 'Electronics'", "'Electronics', 'Music'",
    "'Music', 'Home & Kitchen'", "'Music', 'Toys & Games'")
  val q17_list = for (
    i <- q17_year_list;
    j <- q17_month_list;
    k <- q17_i_category_IN_list
  ) yield (i, j, k)

  //  Q18
  val q18_start_year_list = 2000 to 2004
  val q18_start_month_list = 1 to 5
  val q18_month_gaps = 3 to 6
  val q18_list = for (
    y <- q18_start_year_list;
    m <- q18_start_month_list;
    g <- q18_month_gaps
  ) yield (f"${y}-${m}%02d-02", f"${y}-${m+g}%02d-02")

  //  Q19
  val q19_mondays = List("2004-01-05", "2004-02-09", "2004-03-08", "2004-04-12", "2004-05-24", "2004-06-21",
    "2004-08-02", "2004-11-15", "2004-12-20")
  val q19_date_IN_list = q19_mondays.combinations(4).map(_.map(x => s"'${x}'").mkString(",")).toList

  //  Q20
  val q20_numclust_list = (3 to 12).map(_.toString).toList
  val q20_iter_list = (10 to 50 by 4).map(_.toString).toList
  val q20_list = for (
    i <- q20_numclust_list;
    j <- q20_iter_list
  ) yield (i, j)

  //  Q21
  val q21_year_list = 1995 to 2004
  val q21_month_list = 1 to 10
  val q21_limit = 100
  val q21_list = for (
    i <- q21_year_list;
    j <- q21_month_list
  ) yield (i, j)

  //  Q22
  val q22_date_list = (1 to 20).map(d => f"2001-05-${d}%02d")
  val q22_i_current_price_min = 0.98
  val q22_i_current_price_max_list = (0 to 4).map(x => x * 10 + 1.5)
  val q22_list = for (
    i <- q22_date_list;
    j <- q22_i_current_price_max_list
  ) yield (i, j)

  // Q23
  val q23_year_list = 2001 to 2005
  val q23_month_list = 1 to 10
  val q23_coefficient_list = List(1.2, 1.3)
  val q23_list = for (
    i <- q23_year_list;
    j <- q23_month_list;
    k <- q23_coefficient_list
  ) yield (i, j, k)

  //  Q24
  val q24_i_item_sk_list = 10000 to 10200 by 2

  //  Q25
  val q25_date_list = (1 to 20).map(x => f"2002-01-${x}%02d")
  val q25_numcluster_list = (4 to 12 by 2).map(_.toString)
  val q25_list = for (
    i <- q25_date_list;
    j <- q25_numcluster_list
  ) yield (i, j)


  //  Q26
  val q26_i_category_IN_list = i_categories.combinations(1).map(_.map(x => s"'${x}'").mkString(",")).toList ++
    i_categories.combinations(2).map(_.map(x => s"'${x}'").mkString(",")).toList
  val q26_count_ss_item_sk = 5
  val q26_numcluster_list = List("4", "6", "8", "10", "12")
  val q26_list = for (
    i <- q26_i_category_IN_list;
    j <- q26_numcluster_list
  ) yield (i, j)

  //  Q27
  val q27_pr_item_sk_list = 10000 to 10200 by 2

  //  Q28
  val q28_lambda_list = List("1.0", "0.8", "0.6", "0.4", "0.2")
  val q28_pr_review_rating_IN_list = pr_review_ratings.combinations(5).map(_.mkString(",")).toList ++
    pr_review_ratings.combinations(4).map(_.mkString(",")).toList ++
    pr_review_ratings.combinations(3).map(_.mkString(",")).toList ++
    pr_review_ratings.combinations(2).map(_.mkString(",")).toList
  val q28_list = for (
    i <- q28_lambda_list;
    j <- q28_pr_review_rating_IN_list
  ) yield (i, j)


  //  Q29
  val q29_limit = 100
  val q29_i_category_id_IN_list = i_category_ids.combinations(9).map(_.mkString(",")).toList ++ i_category_ids.combinations(8).map(_.mkString(",")).toList ++  i_category_ids.combinations(7).map(_.mkString(",")).toList ++  i_category_ids.combinations(6).map(_.mkString(",")).toList

  //  Q30
  val q30_limit = 100
  val q30_session_timeout_inSec_list = 1800 to 7800 by 60


}
