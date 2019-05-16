#!/bin/bash
#Check for runtime arguments
SPARK_JOB="spark_job"
if [ $# -gt 0 ]; then
	#Read sparkjob name
	SPARK_JOB="$1"
fi
MODE="default"
#Check run mode provided
if [ $# -gt 1 ]; then
	#Read run mode
	MODE="$2"
fi

SPARK_MASTER ="local"
#Check spark_master provided
if [ $# -gt 2 ]; then
	#Read run mode
	SPARK_MASTER="$3"
fi


#Root folder location of the project
PROJECT_ROOT_FOLDER=$(pwd)
export PROJECT_ROOT_FOLDER=$PROJECT_ROOT_FOLDER

#Set spark configs to env vars
. $PROJECT_ROOT_FOLDER/conf/set-spark-config.sh

#Set egg file path
EGG_FILE="$PROJECT_ROOT_FOLDER/src/dist/*.egg"

#Set script to pass to spark
PYSCRIPT="$PROJECT_ROOT_FOLDER/src/etl/job/$SPARK_JOB.py --config $PROJECT_ROOT_FOLDER/conf/$MODE.config.yml"

#Execute pyspark job
time spark-submit \
	--verbose \
	--conf $SPARK_STRING_MAX_LENGTH \
	--conf $SPARK_DYNAMIC_ALLOCATION \
	--conf $SPARK_SHUFFLE_SERVICE \
	--conf $SPARK_CONSOLE_PROGRESS_UI \
	--conf $SPARK_AUTH_SECRET \
	--conf $SPARK_DRIVER_LOG_CONF \
	--conf $SPARK_EXECUTOR_LOG_CONF \
	--conf $SPARK_YARN_EXECUTOR_MEMORY \
	--conf $SPARK_NETWORK_TIMEOUT \
	--master $SPARK_MASTER \
	--driver-memory $SPARK_DRIVER_MEMORY \
	--executor-memory $SPARK_EXECUTOR_MEMORY \
	--jars $SPARK_INCLUDE_JAR\
	--py-files $EGG_FILE \
	$PYSCRIPT

echo -e "Job $1 completed"
# else
# 	echo -e "ERROR: No Job Name provided, need a Job Name to execute"
# 	echo -e "Usage: sh run-spark-job.sh <M:jobName> <O:mode=default>\n M:Mandatory, O:Optional"
# 	exit -1
# fi