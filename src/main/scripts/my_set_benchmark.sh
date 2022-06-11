bm=$1
sf=$2
jpath=/opt/hex_users/$USER/spark-3.2.1-hadoop3.3.0/jdk1.8
lpath=/opt/hex_users/$USER/chenghao/spark-sql-perf/src/main/resources/log4j.properties

~/spark/bin/spark-submit \
--class com.databricks.spark.sql.perf.MySetBenchmark \
--name $bm_$sf \
--master yarn \
--deploy-mode client \
--conf spark.executorEnv.JAVA_HOME=$jpath \
--conf spark.yarn.appMasterEnv.JAVA_HOME=$jpath \
--conf spark.default.parallelism=240 \
--conf spark.executor.instances=30 \
--conf spark.executor.cores=4 \
--conf spark.executor.memory=81920m \
--conf spark.reducer.maxSizeInFlight=256m \
--driver-java-options "-Dlog4j.configuration=file:$lpath" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
--files "$lpath" \
--jars examples/jars/scopt_2.12-3.7.1.jar \
/opt/hex_users/$USER/chenghao/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
-b $bm -d /mnt/disk7/chenghao-dataset -s $sf -l hdfs://${HOSTNAME}-opa:8020/user/spark_benchmark

#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_set_benchmark.sh TPCH 10
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_set_benchmark.sh TPCH 100
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_set_benchmark.sh TPCH 1000
#
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_set_benchmark.sh TPCDS 10
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_set_benchmark.sh TPCDS 100
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_set_benchmark.sh TPCDS 1000