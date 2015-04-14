#!/usr/bin/python
from modules.sponsor import SpeedtestSponsor
from config_file import sponsors

from datetime import date, timedelta
import logging

logger = logging.getLogger("speedtest_reporting")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('./logs/' + str(date.today())+'.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

yesterday = date.today() - timedelta(1)
str_yesterday = str(yesterday)

# add handler to logger object
logger.addHandler(fh)
logger.info("Program started")
print "Starting"

for sponsor in sponsors:
	sponsor_string = sponsor.get('sponsor_string')
	first_test_date = sponsor.get('first_test_date')
	sponsor_email = sponsor.get('sponsor_email')
	sponsor_password = sponsor.get('sponsor_password')
	
	#Instantiate SpeedtestSponsor
	sponsor= SpeedtestSponsor(sponsor_string, first_test_date,
						 sponsor_email, sponsor_password)
	

	sponsor.downloadMissingTSVs(str_yesterday)
	# PostgreSQL import requires PostGIS
	sponsor.updateLoadTSVs(str_yesterday)

logger.info("Done!")
print "Done!"