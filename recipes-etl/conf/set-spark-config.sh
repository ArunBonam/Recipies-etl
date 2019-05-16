#!/bin/bash

###############################################
# Spark specific arguments and variables
###############################################

# Reading from ES only works with this ES Hadoop + Scala version
export SPARK_INCLUDE_JAR="$PROJECT_ROOT_FOLDER/lib/com.cloudera.impala.jdbc41.Driver","$PROJECT_ROOT_FOLDER/lib/aws-java-sdk-1.7.4.jar",
                          "$PROJECT_ROOT_FOLDER/lib/hadoop-aws-2.7.1.jar"

# Memory to use

export SPARK_DRIVER_MEMORY="20g"
export SPARK_EXECUTOR_MEMORY="16g"

# Spark master to use (local vs yarn)
export SPARK_MASTER="yarn"
# export SPARK_MASTER="local[*]"

# Spark deploy mode (client vs cluster mode)
# export SPARK_DEPLOY_MODE="cluster"

# Spark conf - default partition size
# export SPARK_PARALLELISM="spark.default.parallelism=1"

# nicer debug output
export SPARK_STRING_MAX_LENGTH="spark.debug.maxToStringFields=1000"

# Set these to true when running in Yarn (fixing misconfigured Yarn in DL Dev)
export SPARK_DYNAMIC_ALLOCATION="spark.dynamicAllocation.enabled=true"
export SPARK_SHUFFLE_SERVICE="spark.shuffle.service.enabled=true"
export SPARK_CONSOLE_PROGRESS_UI="spark.ui.showConsoleProgress=true"
export SPARK_AUTH_SECRET="spark.authenticate.secret=true"
export SPARK_DRIVER_LOG_CONF="spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$PROJECT_ROOT_FOLDER/conf/log4j.properties"
export SPARK_EXECUTOR_LOG_CONF="spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$PROJECT_ROOT_FOLDER/conf/log4j.properties"
# Reserved memory for yarn on executor - set (to 600MB) because the executors were lost while processing huge data
export SPARK_YARN_EXECUTOR_MEMORY="spark.yarn.executor.memoryOverhead=1000"
export SPARK_NETWORK_TIMEOUT="spark.network.timeout=1000000"
# export SPARK_CONF9="spark.executor.extraJavaOptions=-Ds3service.server-side-encryption=AES256"
# export SPARK_CONF10="spark.hadoop.fs.s3a.server-side-encryption-algorithm=AES256"

# SPARK environment variables
export SPARK_MAJOR_VERSION=2.2.0
export HADOOP_USER_NAME=hdfs
