__author__ = 'arun-bonam'

import click
from os import path, environ
from traceback import format_exc
from pyspark.sql.functions import lit,regexp_extract,when,unix_timestamp
from etl.job.spark_job import SparkJob
from pyspark import StorageLevel
from etl.util.log import getLocalLogger

JOB_NAME = 'INGRIDIENT-DIFFICULTY-DERIVATION'


@click.command()
@click.option('--config', help='Configuration (.yml) file path', type=str, default=path.
              join(environ.get("PROJECT_ROOT_FOLDER", ""), 'conf/default.config.yml'))

def parseArgs(config):

    try:
        DifficultyDerivation(JOB_NAME, config).run()
    except Exception as e:
        SparkJob.sendEmailNotification( subject='PySpark Job DifficultyDerivation Failed:Unable to parse args',message=e)




class DifficultyDerivation(SparkJob):

    try:
        # Constructor
        def __init__(self, jobName, configFilePath):
            super(DifficultyDerivation, self).__init__(jobName, configFilePath)

        def calculateDifficulty(self, dfIngridients):
            dfIngridients.persist(StorageLevel.MEMORY_AND_DISK)
            dfIngridients =dfIngridients.filter(dfIngridients.ingridents.contains("beef"))
            dfIngridients = dfIngridients.withColumn("cookTimeInt", regexp_extract(dfIngridients.cookTime, "(d+)", 1)) \
                .withColumn("prepTimeInt", regexp_extract(dfIngridients.cookTime, "(d+)", 1))

            dfIngridients = dfIngridients.withColumn("totalTime", dfIngridients.cookTimeInt + dfIngridients.prepTimeInt)

            dfIngridients_filtered = dfIngridients.withColumn("difficulty",
                                                              when(dfIngridients.totalTime > 60, lit("Hard")).when(
                                                                  dfIngridients.totalTime > 30 & dfIngridients.totalTime < 60,
                                                                  lit("Medium")) \
                                                              .when(dfIngridients.totalTime < 30, lit("Easy")) \
                                                              .otherwise(lit("Unknown"))).withColumn("currentDate",
                                                                                                     unix_timestamp() * 1000)

            dfIngridients_filtered.repartition("difficulty").persist(StorageLevel.MEMORY_AND_DISK)

            self.log.info("dfIngridients_filtered: {}".format(dfIngridients_filtered.rdd.count()))

            self.writeToImpala(dataToWrite=dfIngridients_filtered, table=self.config.get('impala').get('tablename'),
                               properties=self.ImpalaProperties.get('impala'))

        def process(self):
            dfIngridients = self.readFromS3(self, self.s3Properties.get('S3'))
            self.log.info("dfIngridients: {}".format(dfIngridients.rdd.count()))
            self.calculateDifficulty(dfIngridients)

    except Exception as e:
        SparkJob.sendEmailNotification(subject='PySpark Job DifficultyDerivation Failed:Error Occured in Processing job', message=e)



if __name__ == '__main__':
    logger = getLocalLogger(JOB_NAME)
    try:
        # Process command line arguments
        parseArgs()
    except Exception as e:
        print(format_exc())
    logger.error(format_exc())

