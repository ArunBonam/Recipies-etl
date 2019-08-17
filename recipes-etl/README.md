# A sample spark etl to transform a json file and apply few transformations
#arun-bonam

This repository contains 
1)Spark job and  utilty  python jobs and shell scripts used for data load and transformation of Recipes.json
  available in S3 Bucket and loads into Impala Table.
2)This spark job can be executed in yarn mode or local mode,instructions given in below.

## Architecture

recipes-etl/src/etl/job:

1)Spark Job:difficulty_derivation avialable in this directory is responsible for calculating Difficulty level of a recipe.
2)Python job:spark_job available here ,is a generic job to handle multiple spark jobs needs (getting variables,reusable methods to read write data)

recipes-etl/bin:

1)run-spark-job.sh : This shell script is responsible to start the spark job using spark submit.


recipes-etl/conf:

1) default-conf.yml : This yaml file contains all the necessary configuration parameters(s3 file path,impala params etc..) which are used across the repo in different cases.
2)set-spark-config.sh : This shell script contains all the environment variables required to run a spark job.


recipes-etl/src/etl/util:

1)email.py:  This conatins a generic class with resusable methods that are used to send email notifications.
2)impala.py : This contains reusable methods to load data into impala.
3)s3.py : This contains resuable methods to read data from S3.
4)spark.py : This contains generic methods to get Spark Context,Spark Config ,Spark Session etc.
5)log.py :  This contains methods 

recipes-etl/src/test:

1)pyspark-app-test.py : This contains test fixtures and assertions for unit testing.
2)s3_test.py : This contains reusable methods to read from s3 if we have multiple spark test files
3)spark_job_test : this contains methods to resue to create variables,sparkContext,sparkSession etc.





## Prerequisites

- Spark v2.0.0 or more
- python 3.7 
-Impala and aws jars in lib folder



## Install

1. Clone this repository
2. Make sure all the prerequisites are available
3. Install the python dependecies

    ```
    pip install -r src/requirements.txt
    ```
4. Fill out connection details in `conf/default.config.yml` 
5. Change Spark related configuration in `bin/set-spark-config.sh` (optional)
6. Build the .egg file - make sure you are in `src` directory when you execute the following command

    ```
     sudo sh create-egg-file.sh
    ```

7. Execute `sh bin/run-spark-job.sh <spark-job> <mode>` - Where spark-job (optional: Specifying which pySpark job to run, find it under `src/etl/job/`, without extension) \

    ```
    # Example to run Master data load job in default (mode) environment (accordingly we can have dev,preprod,prod)
    sh bin/run-spark-job.sh difficulty_derivation default yarn 
    sh bin/run-spark-job.sh difficulty_derivation default local
    ```

Required Jars:
1)com.cloudera.impala.jdbc41.Driver
2)aws-java-sdk-1.7.4.jar
3)hadoop-aws-2.7.1.jar



