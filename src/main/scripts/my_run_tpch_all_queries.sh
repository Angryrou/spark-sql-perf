sf=$1
para=$2

for q in {1..22}
do
  bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCH $sf $para $q
done

