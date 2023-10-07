#Logging module for this custom TCP Load Balancer
import logging

#Define the logging configuration such as log file, file access mode, log format, level, and date format
#Then get the logger instance
#Before running this Load Balancer, please create empty file defined in filename configuration using 'touch' or whatever tools you want
#Example command to create the empty file: 'touch /var/log/custom-lb.log'
logging.basicConfig(filename='/var/log/custom-lb.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s',level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()

#Informational message logging
def LogInfo(message):
	try:
		logger.info(message)

	except Exception as e:
		print (e)

#Debugging message logging
def LogDebug(message):
	try:
		logger.debug(message)

	except Exception as e:
		print (e)

#Warning message logging
def LogWarning(message):
	try:
		logger.warning(message)

	except Exception as e:
		print (e)

#Error message logging
def LogError(message):
	try:
		logger.error(message)

	except Exception as e:
		print (e)

#Critical message logging
def LogCritical(message):
	try:
		logger.critical(message)

	except Exception as e:
		print (e)


