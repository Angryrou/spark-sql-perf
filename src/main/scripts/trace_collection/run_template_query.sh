tid=$1
qid=$2
qheader=$3

cpe=5 # core per exec
mpe=32 # mem per exec = 4 core x 8G/core = 32G
nexec=8

ncores=$(($cpe * $nexec))
npara=$ncores
spara=$ncores

bm=TPCH
sf=100
jpath=/opt/hex_users/$USER/spark-3.2.1-hadoop3.3.0/jdk1.8
lpath=/opt/hex_users/$USER/chenghao/spark-sql-perf/src/main/resources/log4j.properties


~/spark/bin/spark-submit \
--class com.databricks.spark.sql.perf.MyRunTemplateQuery \
--name q${tid}-${qid} \
--master yarn \
--deploy-mode client \
--conf spark.executorEnv.JAVA_HOME=${jpath} \
--conf spark.yarn.appMasterEnv.JAVA_HOME=${jpath} \
--conf spark.default.parallelism=${npara} \
--conf spark.sql.shuffle.partitions=${spara} \
--conf spark.executor.instances=${nexec} \
--conf spark.executor.cores=${cpe} \
--conf spark.executor.memory=${mpe}g \
--conf spark.yarn.am.cores=${cpe} \
--conf spark.yarn.am.memory=${mpe}g \
--conf spark.driver.cores=${cpe} \
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
-b $bm -t $tid -q $qid -s $sf -l $qheader > ${tid}-${qid}.log 2>&1


# bash ~/chenghao/spark-sql-perf/src/main/scripts/benchmark_concurrent_testing/run_unit.sh FIFO

