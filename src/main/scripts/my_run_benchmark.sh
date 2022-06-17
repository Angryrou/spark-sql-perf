bm=$1
sf=$2
nexec=$3
npara=$4
spara=$5

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
--class com.databricks.spark.sql.perf.MyRunBenchmark \
--name ${bm}_${sf}_run_p${npara}_sp=${spara} \
--master yarn \
--deploy-mode client \
--conf spark.executorEnv.JAVA_HOME=${jpath} \
--conf spark.yarn.appMasterEnv.JAVA_HOME=${jpath} \
--conf spark.default.parallelism=${npara} \
--conf spark.sql.shuffle.partitions=${spara} \
--conf spark.executor.instances=${nexec} \
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
--conf spark.sql.files.openCostInBytes=134217728 \
--conf spark.sql.broadcastTimeout=10000 \
--driver-java-options "-Dlog4j.configuration=file:$lpath" \
--conf "spark.driver.extraJavaOptions=-Xms20g" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
--files "$lpath" \
--jars ~/spark/examples/jars/scopt_2.12-3.7.1.jar \
/opt/hex_users/$USER/chenghao/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
-b $bm -s $sf -l hdfs://${HOSTNAME}-opa:8020/user/spark_benchmark

#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 100 4 32 32 # 16 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 100 8 64 64 # 32 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 100 16 128 128 # 64 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 100 32 256 256 # 128 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 1000 4 32 32 # 16 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 1000 8 64 64 # 32 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 1000 16 128 128 # 64 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 1000 32 256 256 # 128 cores

#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 100 4 16 16 # 16 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 100 8 32 32 # 32 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 100 16 64 64 # 64 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 100 32 128 128 # 128 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 1000 4 16 16 # 16 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 1000 8 32 32 # 32 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 1000 16 64 64 # 64 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 1000 32 128 128 # 128 cores

#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 100 4 32 32 # 16 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 100 8 64 64 # 32 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 100 16 128 128 # 64 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 100 32 256 256 # 128 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 1000 4 32 32 # 16 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 1000 8 64 64 # 32 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 1000 16 128 128 # 64 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 1000 32 256 256 # 128 cores

#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 100 4 16 16 # 16 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 100 8 32 32 # 32 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 100 16 64 64 # 64 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 100 32 128 128 # 128 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 1000 4 16 16 # 16 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 1000 8 32 32 # 32 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 1000 16 64 64 # 64 cores
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 1000 32 128 128 # 128 cores
