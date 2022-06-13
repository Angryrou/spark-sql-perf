bash ~/chenghao/spark-sql-perf/src/main/scripts/my_set_benchmark.sh TPCH 3000
bash ~/chenghao/spark-sql-perf/src/main/scripts/my_set_benchmark.sh TPCH 10000
bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 3000 50
bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 10000 50


bash ~/chenghao/spark-sql-perf/src/main/scripts/my_set_benchmark.sh TPCDS 3000
bash ~/chenghao/spark-sql-perf/src/main/scripts/my_set_benchmark.sh TPCDS 10000
bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 3000 50
bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 10000 50

