__author__ = 'arun-bonam'

import logging
from os import path, environ, getcwd
from datetime import date as ddate


#For dev purpose only as it logs to a file
def getLocalLogger(jobName, level=logging.INFO):
	logger = logging.getLogger(jobName)
	logger.setLevel(level)
	if logger.handlers:
		pass
	else:
		logFolder = path.join(environ.get('PROJECT_ROOT_FOLDER',getcwd()),'out/log')
		logfile = path.join(logFolder,'{}.txt'.format(ddate.today().strftime("%Y-%m-%d")))
		ch = logging.FileHandler(logfile)
		ch.setLevel(level)
		formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		ch.setFormatter(formatter)
		logger.addHandler(ch)
	return logger


def getSparkLogger(sc,jobName):
	log4jLogger = sc._jvm.org.apache.log4j
	return log4jLogger.LogManager.getLogger(jobName)