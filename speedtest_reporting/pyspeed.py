#!/usr/bin/python
from lib.sponsor import SpeedtestSponsor
from lib.parse import createServerTable
from configs import sponsors

from datetime import date, timedelta
import logging

today = date.today()
logger = logging.getLogger("pyspeed")
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('./logs/' + str(today)+'.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)

yesterday = date.today() - timedelta(1)
print date.today()
print yesterday
str_yesterday = str(yesterday)

# add handler to logger object
logger.addHandler(fh)
logger.info("Program started")
print "Starting"

## RUN THIS TO CREATE/UPDATE speedtest_servers table
## UNCOMMENT LINE BELOW TO RUN
# createServerTable()

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