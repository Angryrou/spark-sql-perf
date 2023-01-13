package chenghao.tpcxbb

object Paras {

// Q1
//  val q01_i_category_id_IN="1, 2 ,3"
  val q01_i_category_id_IN_list = List("1,2,3", "2,3,4", "3,4,5", "4,5,6", "5,6,7")
  val q01_ss_store_sk_IN="10, 20, 33, 40, 50"
  val q01_viewed_together_count=50
//  val q01_limit=100
  val q01_limit_list = List(100, 50, 150, 70, 80, 90, 110, 120)

//  Q2
//  val q02_item_sk=10001
  val q02_item_sk_list = List(10001, 10002, 10003, 10004, 10005)
//  val q02_limit=30
  val q02_limit_list = List(30, 15, 45, 20, 25, 35, 40, 50)
  val q02_session_timeout_inSec=3600

//  Q3
  val q03_days_in_sec_before_purchase=864000
  val q03_views_before_purchase=5
//  val q03_purchased_item_IN=10001
  val q03_purchased_item_IN_list = List(10001, 10002, 10003, 10004, 10005)
  val q03_purchased_item_category_IN="2,3"
//  val q03_limit=100
  val q03_limit_list = List(100, 50, 150, 70, 80, 90, 110, 120)

//  Q4
  val q04_session_timeout_inSec=3600
  // q04_wcs_click_date -> min: 36890, max: 38697, count: 1808, step: 1
  // q04_wcs_click_date_upper_list is going to choose 30%, 40%, ..., 100% (8 choices)
  val q04_wcs_click_date_upper_list = List(-1, 38097, 37497, 37670, 37840, 38183, 38355, 38526)
  val q04_additional_time_pressure_rate_list = List(0, 0.5, 1, 0.25, 0.75)


//  Q5
//  val q05_i_category="'Books'"
//  Count(*) group by i_category. 'Books': 52870, 'Music': 81028, 'Movies & TV': 13369, others: 4k ~ 8k
  val q05_i_category_list=List("'Books'", "'Music'", "'Movies & TV'", "'Electronics'", "'Clothing & Accessories'")
  val q05_cd_education_status_IN="'Advanced Degree', 'College', '4 yr Degree', '2 yr Degree'"
  val q05_cd_gender="'M'"
  val q05_lambda_list = List("0.0", "0.5", "1.0", "0.3", "0.4", "0.6", "0.7", "0.8")

//  Q6
  val q06_limit_list=List(100, 50, 150, 70, 80, 90, 110, 120)
  val q06_year_list=List(2001, 2002, 2003, 2004, 2005)

//  Q7
  val q07_HIGHER_PRICE_RATIO=1.2
//  val q07_YEAR=2004
  val q07_year_list=List(2004, 2005, 2006, 2007, 2008)
  val q07_MONTH=7
  val q07_HAVING_COUNT_GE=10
//  val q07_LIMIT=10
  val q07_limit_list=List(10, 5, 15, 7, 8, 9, 11, 12)

//  Q8
  val q08_startDate_list=List("2001-09-02","2002-09-02", "2003-09-02",
    "2001-12-02", "2002-03-02", "2002-06-02", "2002-12-02", "2003-03-02")
  val q08_endDate_list=List("2002-09-02", "2003-09-02", "2004-09-02",
    "2002-12-02", "2003-03-02", "2003-06-02", "2003-12-02", "2004-03-02")
  val q08_seconds_before_purchase_list=List(259200, 172800, 345600, 216000, 302400)

//  Q9
  val q09_year_list=List(2001, 2002, 2003, 1999, 2000, 2004, 2005, 2006)

  val q09_marital_status_list = List("M", "U", "D", "W", "S")

  val q09_part1_ca_country="United States"
  val q09_part1_ca_state_IN="'KY', 'GA', 'NM'"
  val q09_part1_net_profit_min=0
  val q09_part1_net_profit_max=2000
  val q09_part1_education_status="4 yr Degree"
//  val q09_part1_marital_status="M"
  val q09_part1_sales_price_min=100
  val q09_part1_sales_price_max=150

  val q09_part2_ca_country="United States"
  val q09_part2_ca_state_IN="'MT', 'OR', 'IN'"
  val q09_part2_net_profit_min=150
  val q09_part2_net_profit_max=3000
  val q09_part2_education_status="4 yr Degree"
//  val q09_part2_marital_status="M"
  val q09_part2_sales_price_min=50
  val q09_part2_sales_price_max=200

  val q09_part3_ca_country="United States"
  val q09_part3_ca_state_IN="'WI', 'MO', 'WV'"
  val q09_part3_net_profit_min=50
  val q09_part3_net_profit_max=25000
  val q09_part3_education_status="4 yr Degree"
//  val q09_part3_marital_status="M"
  val q09_part3_sales_price_min=150
  val q09_part3_sales_price_max=200

//  Q10
  // add by chenghao
  val q10_limit_list = List(-1, 10000, 1000,
    1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500)

//  Q11
  val q11_startDate_list=List("2003-01-02", "2003-02-02", "2003-03-02",
    "2002-03-02", "2002-04-02", "2002-05-02", "2002-06-02", "2002-07-02", "2002-08-02", "2002-09-02", "2002-10-02",
    "2002-11-02", "2002-12-02",
    "2003-04-02", "2003-05-02", "2003-06-02", "2003-07-02", "2003-08-02", "2003-09-02", "2003-10-02")

  val q11_endDate_list=List("2003-02-02", "2003-03-02", "2003-04-02",
    "2002-04-02", "2002-05-02", "2002-06-02", "2002-07-02", "2002-08-02", "2002-09-02", "2002-10-02", "2020-11-02",
    "2002-12-02", "2003-01-02",
    "2003-05-02", "2003-06-02", "2003-07-02", "2003-08-02", "2003-09-02", "2003-10-02", "2003-11-02")

//  Q12
  val q12_i_category_IN_list=List("'Books', 'Electronics'", "'Books', 'Music'", "'Electronics', 'Music'",
  "'Music', 'Home & Kitchen'", "'Music', 'Toys & Games'")
  // add by chenghao
  val q12_limit_list=List(-1, 1000, 100, 200, 300, 400, 500, 600)

//  Q13
  val q13_year_list= List(2001, 2002, 2003, 2004, 2005)
  val q13_limit_list=List(100, 50, 150, 70, 80, 90, 110, 120)

//  Q14
  val q14_dependents_list=List(5, 2, 8, 3, 4, 6, 7, 9)
  val q14_morning_startHour=7
  val q14_morning_endHour=8
  val q14_evening_startHour=19
  val q14_evening_endHour=20
  val q14_content_len_min_list=List(5000, 4000, 3000, 4500, 3500)
  val q14_content_len_max_list=List(6000, 5000, 4000, 5500, 4500)

//  Q15
  val q15_startDate_list=List("2001-09-02", "2002-09-02", "2003-09-02",
    "2001-12-02", "2002-03-02", "2002-06-02", "2002-12-02", "2003-03-02")
  val q15_endDate_list=List("2002-09-02", "2003-09-02", "2004-09-02",
    "2002-12-02", "2003-03-02", "2003-06-02", "2003-12-02", "2004-03-02")
  val q15_store_sk_list=List(10, 5, 0, 15, 20)

//  Q16
  val q16_date_list=List("2001-03-16", "2001-02-16", "2001-04-16", "2001-02-26", "2001-03-26")
  // add by chenghao
  val q16_limit_list=List(100, 50, 150, 70, 80, 90, 110, 120)

//  Q17
  val q17_gmt_offset="-5"
  val q17_year=2001
  val q17_month_list=List(12, 8, 4, 1, 3, 5, 7, 9)
//  val q17_i_category_IN="'Books', 'Music'"
  val q17_i_category_IN_list=List("'Books', 'Music'", "'Books', 'Electronics'", "'Electronics', 'Music'",
    "'Music', 'Home & Kitchen'", "'Music', 'Toys & Games'")

//  Q18
  val q18_startDate_list=List("2001-05-02", "2001-09-02", "2012-01-02", "2001-07-02", "2001-11-02")
  val q18_endDate_list=List("2001-09-02", "2012-01-02", "2012-05-02", "2001-11-02", "2001-03-02")
  // add by chenghao
  val q18_limit_list = List(-1, 1000, 100, 200, 300, 400, 500, 600)


//  Q19
//  val q19_storeReturns_date_IN="'2004-03-08' ,'2004-08-02' ,'2004-11-15', '2004-12-20'"
  val q19_storeReturns_date_IN_list = List("'2004-03-08' ,'2004-08-02' ,'2004-11-15', '2004-12-20'",
    "'2004-03-09' ,'2004-08-03' ,'2004-11-16', '2004-12-21'", "'2004-03-10' ,'2004-08-04' ,'2004-11-17', '2004-12-22'",
    "'2004-03-11' ,'2004-08-05' ,'2004-11-18', '2004-12-23'", "'2004-03-12' ,'2004-08-06' ,'2004-11-19', '2004-12-24'")

  val q19_webReturns_date_IN_list = List("'2004-03-08' ,'2004-08-02' ,'2004-11-15', '2004-12-20'",
    "'2004-03-09' ,'2004-08-03' ,'2004-11-16', '2004-12-21'", "'2004-03-10' ,'2004-08-04' ,'2004-11-17', '2004-12-22'",
    "'2004-03-11' ,'2004-08-05' ,'2004-11-18', '2004-12-23'", "'2004-03-12' ,'2004-08-06' ,'2004-11-19', '2004-12-24'")
  val q19_store_return_limit_list=List(-1, 100, 50, 65, 70, 75, 80, 85)

//  Q20
  val q20_numclust_list = List("8", "4", "12", "5", "6", "7", "9", "10")
  val q20_iter_list = List("20", "30", "10", "25", "15")

//  Q21
  val q21_year_list=List(2003, 2004, 2005, 2001, 2002)
  val q21_month=1
  val q21_limit_list=List(100, 50, 150, 70, 80, 90, 110, 120)

//  Q22
  val q22_date_list=List("2001-05-08", "2001-05-09", "2001-05-10",
    "2001-05-11", "2001-05-12", "2001-05-07", "2001-05-06", "2001-05-05")
  val q22_i_current_price_min_list=List(0.98, 0.88, 1.08, 0.93, 1.03)
  val q22_i_current_price_max=1.5

// Q23
  val q23_year_list=List(2001, 2002, 2003, 1999, 2000, 2004, 2005, 2006)
  val q23_month_list=List(1, 2, 3, 4, 5)
  val q23_coefficient=1.3

//  Q24
  val q24_i_item_sk_list=List(10000, 10001, 10007, 10002, 10003, 10004, 10005, 10006)
  val q24_limit_list = List(-1, 4, 2, 1, 3)

//  Q25
  val q25_date_list=List("2002-01-02", "2002-01-03", "2002-01-04",
    "2002-01-05", "2002-01-06", "2002-01-07", "2002-01-08", "2002-01-09")
  val q25_numcluster_list=List("8", "4", "12", "6", "10")

//  Q26
  val q26_i_category_IN_list=List("'Books'", "'Electronics'", "'Music'",
    "'Movies & TV'", "'Clothing & Accessories'")
  val q26_count_ss_item_sk=5
  val q26_numcluster_list=List("8", "4", "12", "5", "6", "7", "9", "10")

//  Q27
  val q27_pr_item_sk_list= List(10002, 10003, 10004, 10005, 10006)
  val q27_limit_list = List(-1, 500, 100, 200, 300, 400, 250, 350)

//  Q28
  val q28_lambda_list = List("0.0", "0.5", "1.0", "0.2", "0.3", "0.4", "0.6", "0.7")
  val q28_additional_time_pressure_rate_list = List(0, 0.5, 1, 0.25, 0.75)

  //  Q29
  val q29_limit_list=List(100, 50, 100, 70, 80, 90, 110, 120)
  val q29_ws_quantity_upper_list = List(-1, 15, 10, 8, 12)

//  Q30
  val q30_limit_list=List(100, 50, 100, 70, 80, 90, 110, 120)
  val q30_wcs_click_date_upper_list = List(-1, 38097, 37497, 37670, 38355)
  val q30_session_timeout_inSec=3600


}
