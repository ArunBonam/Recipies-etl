__author__ = 'arun_bonam'

import yaml
import click
from sys import exit
from os import path, environ
from traceback import format_exc

from etl.util.spark import getSparkContext, getSqlContext, getSparkSession
from etl.util.s3 import S3Reader,getS3Properties
from etl.util.impala import getImpalaProperties,dfWriter
from etl.util.email import Gmail
from etl.util.log import getSparkLogger



JOB_NAME = 'SPARK-JOB'

@click.command()
@click.option('--config', help='Configuration (.yml) file path', type=str, \
	default=path.join(environ.get("PROJECT_ROOT_FOLDER", ""),'conf/default.config.yml'))

def parseArgs(config):
	SparkJob(JOB_NAME,config).run()

class SparkJob(object):
	# Constructor
	def __init__(self, jobName, configFilePath):
		self.jobName = jobName
		self.configPath = configFilePath
		self.parseConfig()
		self.sc = getSparkContext(self.jobName)
		self.spark = getSparkSession(self.jobName)
		self.sqlContext = getSqlContext(self.sc)
		self.log = getSparkLogger(self.sc, self.jobName)
		self.s3Properties = getS3Properties(self,self.config.get('s3_prpoertes'))
		self.impalaProperties =getImpalaProperties(self,self.config.get('impala_properties'))
		self.emailid =self.config.get('email').get('id')
		self.email_password =self.config.get('email').get('password')
		self.spark.sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
		self.spark.sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId",self.s3Properties.get('s3').get('s3.acces_id'))
		self.spark.sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey",self.s3Properties.get('s3').get('s3.access_key'))





	# Read config.yml

	def parseConfig(self):
		with open(self.configPath, 'r') as configYml:
			try:
				self.config = yaml.load(configYml)
			except yaml.YAMLError as exc:
				raise exc
		if 's3.file' not in self.config:
			raise Exception('Failed to parse configuration file! Missing s3 tag.')

		elif 'email' not in self.config:
			raise Exception('Failed to parse configuration file! Missing email tag.')

		elif 'impala' not in self.config:
			raise Exception('Failed to parse configuration file! Missing impala tag.')

		else:
			pass

	# Everything is set, now process the job
	def run(self):
		try:
			self.process()
			exit(0)
		except Exception as e:
			self.log.error(format_exc())
			raise e

	def process(self):
		print('Hello from Spark-Job process method')

	def readFromS3(self,properties):
		return S3Reader(self.spark, properties)

	def writeToImpala(self,df,table,properties,mode='append'):
		return dfWriter(df,table,properties,mode)

	def sendEmailNotification(self,subject,message):
		Gmail(self.emailid,self.email_password,subject,message)


if __name__ == '__main__':
	# logger = getLocalLogger(jobName)
	try:
		# Process command line arguments
		parseArgs()
	except Exception as e:
		print(format_exc())