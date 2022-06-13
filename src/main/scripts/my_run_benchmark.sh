bm=$1
sf=$2
para=$3


# md values
# 1 core -> 8G mem
# 1 stage -> 10-50 instances

cpe=4 # core per exec
mpe=32 # mem per exec = 4 core x 8G/core = 32G
ninsts=35 # 7x5


jpath=/opt/hex_users/$USER/spark-3.2.1-hadoop3.3.0/jdk1.8
lpath=/opt/hex_users/$USER/chenghao/spark-sql-perf/src/main/resources/log4j.properties

# default
# spark.sql.files.maxPartitionBytes=128M
# spark.sql.shuffle.partitions=200

~/spark/bin/spark-submit \
--class com.databricks.spark.sql.perf.MyRunBenchmark \
--name ${bm}_${sf}_run_para=${para} \
--master yarn \
--deploy-mode client \
--conf spark.executorEnv.JAVA_HOME=$jpath \
--conf spark.yarn.appMasterEnv.JAVA_HOME=$jpath \
--conf spark.default.parallelism=$para \
--conf spark.sql.shuffle.partitions=$para \
--conf spark.executor.instances=35 \
--conf spark.executor.cores=${cpe} \
--conf spark.executor.memory=${mpe}g \
--conf spark.driver.memory=${mpe}g \
--driver-java-options "-Dlog4j.configuration=file:$lpath" \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
--files "$lpath" \
--jars ~/spark/examples/jars/scopt_2.12-3.7.1.jar \
/opt/hex_users/$USER/chenghao/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
-b $bm -d /mnt/disk7/chenghao-dataset -s $sf -l hdfs://${HOSTNAME}-opa:8020/user/spark_benchmark -n new_${bm}_${sf}

#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 10 20
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 10 50
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 10 100
#
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 100 20
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 100 50
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 100 100
#
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 1000 20
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 1000 50
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCH 1000 100
#
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 10 20
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 10 50
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 10 100
#
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 100 20
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 100 50
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 100 100
#
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 1000 20
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 1000 50
#bash ~/chenghao/spark-sql-perf/src/main/scripts/my_run_benchmark.sh TPCDS 1000 100