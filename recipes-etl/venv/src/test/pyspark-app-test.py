import pytest
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from src.test.spark_job_test import SparkJob
import click
from os import environ,path


@pytest.fixture(scope="session",
                params=[pytest.mark.spark_local('local'),
                        pytest.mark.spark_yarn('yarn')])
def spark_session(request):
    """Fixture for creating a spark session."""

    spark = (SparkSession
             .builder
             .master('local[2]')
             .config('spark.jars.packages', 'com.databricks:spark-avro_2.11:3.0.1')
             .appName('pytest-pyspark-local-testing')
             .getOrCreate())
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "s3AccessKey")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "s3SecretKey")

    request.addfinalizer(lambda: spark.stop())


    return spark


def validate_counts(spark_session):
    df =spark_session.read.json('s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json')
    assert  df.count()>0

@click.command()
@click.option('--config', help='Configuration (.yml) file path', type=str, default=path.
              join(environ.get("PROJECT_ROOT_FOLDER", ""), 'conf/default.config.yml'))
def validate_counts_s3vsimpala(spark_session,config):
    df_s3 = spark_session.read.json('s3://s3-eu-west-1.amazonaws.com/dwh-test-resources/recipes.json')
    url= config.get('database').get('impala').get('host') +":"+config.get('database').get('impala').get('port') +"/"+config.get('dbname')

    df_impala = spark_session.read.jdbc(table=config.get('impala.table'),url=url)
    df_s3_filtered=df_s3.filter(df_s3.ingridient.contains('beef')).count()

    impala_count=df_impala.count()
    s3_count = df_s3.count()

    assert impala_count == s3_count





