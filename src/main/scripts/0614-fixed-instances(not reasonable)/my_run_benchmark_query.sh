bm=$1
sf=$2
para=$3
qname=$4


# md values
# 1 core -> 8G mem
# 1 stage -> 10-50 instances

cpe=4 # core per exec
mpe=32 # mem per exec = 4 core x 8G/core = 32G

jpath=/opt/hex_users/$USER/spark-3.2.1-hadoop3.3.0/jdk1.8
lpath=/opt/hex_users/$USER/chenghao/spark-sql-perf/src/main/resources/log4j.properties

# default
# spark.sql.files.maxPartitionBytes=128M
# spark.sql.shuffle.partitions=200

~/spark/bin/spark-submit \
--class com.databricks.spark.sql.perf.MyRunQuery \
--name ${bm}_${sf}_p${para}_${qname} \
--master yarn \
--deploy-mode client \
--conf spark.executorEnv.JAVA_HOME=$jpath \
--conf spark.yarn.appMasterEnv.JAVA_HOME=$jpath \
--conf spark.default.parallelism=$para \
--conf spark.executor.instances=35 \
--conf spark.executor.cores=${cpe} \
--conf spark.executor.memory=${mpe}g \
--conf spark.yarn.am.cores=5 \
--conf spark.yarn.am.memory=${mpe}g \
--conf spark.driver.cores=5 \
--conf spark.driver.memory=${mpe}g \
--conf spark.reducer.maxSizeInFlight=256m \
--conf spark.rpc.askTimeout=12000 \
--conf spark.shuffle.io.retryWait=60 \
--conf spark.sql.parquet.compression.codec=snappy \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryoserializer.buffer.max=512m \
--conf spark.sql.broadcastTimeout=10000 \
--driver-java-options "-Dlog4j.configuration=file:$lpath" \
--conf "spark.driver.extraJavaOptions=-Xms20g" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
--files "$lpath" \
--jars ~/spark/examples/jars/scopt_2.12-3.7.1.jar \
/opt/hex_users/$USER/chenghao/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
-b $bm -q $qname -s $sf -l hdfs://${HOSTNAME}-opa:8020/user/spark_benchmark

#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCH 10 20
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCH 10 50
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCH 10 100
#
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCH 100 20
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCH 100 50 22
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCH 100 100
#
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCH 1000 20
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCH 1000 50
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCH 1000 100
#
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCDS 10 20
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCDS 10 50
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCDS 10 100
#
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCDS 100 20
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCDS 100 50
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCDS 100 100
#
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCDS 1000 20
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCDS 1000 50
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark_query.sh TPCDS 1000 100