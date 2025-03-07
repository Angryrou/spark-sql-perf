bm=$1
sf=$2
jpath=/opt/hex_users/$USER/spark-3.2.1-hadoop3.3.0/jdk1.8
lpath=/opt/hex_users/$USER/chenghao/spark-sql-perf/src/main/resources/log4j.properties

~/spark/bin/spark-submit \
--class com.databricks.spark.sql.perf.MySetBenchmark \
--name ${bm}_${sf} \
--master yarn \
--deploy-mode client \
--conf spark.executorEnv.JAVA_HOME=$jpath \
--conf spark.yarn.appMasterEnv.JAVA_HOME=$jpath \
--conf spark.default.parallelism=140 \
--conf spark.executor.instances=35 \
--conf spark.executor.cores=4 \
--conf spark.executor.memory=80g \
--conf spark.yarn.am.cores=5 \
--conf spark.yarn.am.memory=80g \
--conf spark.driver.cores=5 \
--conf spark.driver.memory=80g \
--conf spark.reducer.maxSizeInFlight=256m \
--conf spark.rpc.askTimeout=12000 \
--conf spark.shuffle.io.retryWait=60 \
--conf spark.sql.parquet.compression.codec=snappy \
--conf spark.sql.autoBroadcastJoinThreshold=200m \
--driver-java-options "-Dlog4j.configuration=file:$lpath" \
--conf "spark.driver.extraJavaOptions=-Xms20g" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
--files "$lpath" \
--jars ~/spark/examples/jars/scopt_2.12-3.7.1.jar \
/opt/hex_users/$USER/chenghao/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
-b $bm -d /mnt/disk7/chenghao-dataset -s $sf -l hdfs://${HOSTNAME}-opa:8020/user/spark_benchmark -o true

#bash ~/chenghao/spark-sql-perf/src/main/scripts/benchmark_sf_testing/my_set_benchmark.sh TPCH 10
#bash ~/chenghao/spark-sql-perf/src/main/scripts/benchmark_sf_testing/my_set_benchmark.sh TPCH 100
#bash ~/chenghao/spark-sql-perf/src/main/scripts/benchmark_sf_testing/my_set_benchmark.sh TPCH 1000
#bash ~/chenghao/spark-sql-perf/src/main/scripts/benchmark_sf_testing/my_set_benchmark.sh TPCH 3000
#bash ~/chenghao/spark-sql-perf/src/main/scripts/benchmark_sf_testing/my_set_benchmark.sh TPCH 10000
#
#bash ~/chenghao/spark-sql-perf/src/main/scripts/benchmark_sf_testing/my_set_benchmark.sh TPCDS 10
#bash ~/chenghao/spark-sql-perf/src/main/scripts/benchmark_sf_testing/my_set_benchmark.sh TPCDS 100
#bash ~/chenghao/spark-sql-perf/src/main/scripts/benchmark_sf_testing/my_set_benchmark.sh TPCDS 1000
#bash ~/chenghao/spark-sql-perf/src/main/scripts/benchmark_sf_testing/my_set_benchmark.sh TPCDS 3000
#bash ~/chenghao/spark-sql-perf/src/main/scripts/benchmark_sf_testing/my_set_benchmark.sh TPCDS 10000