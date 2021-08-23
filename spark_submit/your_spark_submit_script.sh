#!/bin/bash

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null && pwd )"

#----------------------------------------------------------------------------------+


export HADOOP_HOME=
export JAVA_HOME=
export HADOOP_CONF_DIR=
export YARN_CONF_DIR=

sparkversion=""

HDFS_HOME=

JAR_FILE=${HDFS_HOME}/s2k.jar
LOG4J_FILE=${HDFS_HOME}/log4j-spark.properties
PREF_FILE=${HDFS_HOME}/preferences/s2k.pref

ITOPIC=
DTOPIC=
KUDU_TABLE=

EX_MEM=1g
DRV_MEM=1g
JOB=

# Job specific conf
PRINC=
UNAME=
QUEUE=
KEYTB=/opt/kerberos/keytabs/your.keytab

JOBNAME=`echo $JOB|tr "." " "|awk '{print $NF}'`


spark_submit () {
$sparkversion --master yarn --deploy-mode cluster \
        --class $JOB \
        --files hdfs://$LOG4J_FILE \
        --name yourjob-monpoc-${ITOPIC} \
        --conf spark.executorEnv.JAVA_HOME=${JAVA_HOME} \
        --driver-memory $DRV_MEM \
        --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j-spark.properties -Dconfig.file.path=hdfs://$CONF_FILE" \
        --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j-spark.properties -Dconfig.file.path=hdfs://$CONF_FILE" \
        --conf spark.yarn.maxAppAttempts=4 \
        --conf spark.yarn.am.attemptFailuresValidityInterval=1h \
        --conf spark.yarn.max.executor.failures=8 \
        --conf spark.yarn.executor.failuresValidityInterval=1h \
        --conf spark.task.maxFailures=8 \
        --conf spark.speculation=true \
        --conf spark.hadoop.fs.hdfs.impl.disable.cache=true \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.yarn.executor.memoryOverhead=512 \
        --conf spark.driver.memory=$DRV_MEM \
        --conf spark.ego.uname=$UNAME \
        --conf spark.yarn.submit.waitAppCompletion=false \
	--conf spark.streaming.kafka.maxRatePerPartition=5000 \
	--conf spark.metrics.conf=/opt/spark-metrics/file.properties \
        --num-executors 5 \
	--executor-memory $EX_MEM \
        --queue $QUEUE \
        --principal $PRINC \
        --keytab $KEYTB \
	hdfs://$JAR_FILE \
	--broker BROKER:PORT \
	--auto-offset-reset largest \
	--ingestion-topic opt_out_${ITOPIC} \
        --destination-topic in_${DTOPIC} \
	--checkpoint-folder /tmp/dehli_de/ \
        --offsets-table impala::db_kudu.table \
	--batch-period 10000 \
	--kudu-master MASTER:PORT \
	--primary-key-field ROW_ID \
	--kudu-siebel-field-map ${PREF_FILE} \
	--group-id group_id_${ITOPIC}_200 \
	--skipped-messages-topic skip_${DTOPIC} \
        --stop-on-auth-error true \
	--kudu-table impala::db_kudu.${KUDU_TABLE}
}

spark_submit
