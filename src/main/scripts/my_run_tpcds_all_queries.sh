sf=$1
para=$2

for q in {1..99}
do
  bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCDS $sf $para q$q
done

bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCDS $sf $para ss_max

