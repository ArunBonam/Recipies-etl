__author__ = 'arun-bonam'

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession


def getSparkConf(jobName):
	return (SparkConf().setAppName(jobName))


def getSparkContext(jobName):
	return (SparkContext(conf=getSparkConf(jobName)))


def getSparkSession(jobName):
	return (SparkSession.builder.appName(jobName).getOrCreate())


def getSqlContext(sc):
	return (SQLContext(sc))