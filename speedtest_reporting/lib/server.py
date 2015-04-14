import psycopg2
import sys
import logging
from configs import pg_config

reload(sys)

module_logger = logging.getLogger("pyspeed.Server")

pg_host = pg_config.get('pg_host')
pg_database = pg_config.get('pg_database')
pg_user = pg_config.get('pg_user')
pg_password = pg_config.get('pg_password')

def makeInsert(server_name, server_id, device, simple_sponsor, startdate, enddate):
	if device in 'web_browser':
		SERVER_INSERT = """INSERT INTO geo_""" + str(server_id) + """_web_browser
		(client_ip, isp, test_date,
		download_kbps, upload_kbps, latency, latitude, longitude,
		geom, user_agent)
		SELECT "CLIENT_IP", "ISP", "TEST_DATE",
		"DOWNLOAD_KBPS", "UPLOAD_KBPS", "LATENCY", "LATITUDE", "LONGITUDE",
		ST_SetSRID(ST_MakePoint("LONGITUDE", "LATITUDE"), 4326), "USER_AGENT"
		FROM temp_""" + simple_sponsor + """web_browser
		WHERE "SERVER_NAME" LIKE """ + server_name
		SERVER_INSERT+=""" and "TEST_DATE" BETWEEN '""" + str(startdate) + "' and '" + str(enddate) + "'"
		SERVER_INSERT += """ ORDER BY "TEST_DATE"; """
	else:
		SERVER_INSERT = """INSERT INTO geo_""" + str(server_id) + """_mobile
		(client_ip, isp, test_date,
		download_kbps, upload_kbps, latency, latitude, longitude,
		geom, connection_type, device)
		SELECT * FROM (
		SELECT "CLIENT_IP", "ISP", "TEST_DATE",
		"DOWNLOAD_KBPS", "UPLOAD_KBPS", "LATENCY", "LATITUDE", "LONGITUDE",
		ST_SetSRID(ST_MakePoint("LONGITUDE", "LATITUDE"), 4326), "CONNECTION_TYPE",
		'android' AS device
		FROM temp_""" + simple_sponsor + """android"""
		SERVER_INSERT += """ WHERE "SERVER_NAME" LIKE
		"""
		SERVER_INSERT += server_name
		SERVER_INSERT += """ and "TEST_DATE" BETWEEN '""" + str(startdate) + "' and '" + str(enddate) + "'"
		SERVER_INSERT += """
		UNION
		SELECT "CLIENT_IP", "ISP", "TEST_DATE",
		"DOWNLOAD_KBPS", "UPLOAD_KBPS", "LATENCY", "LATITUDE", "LONGITUDE",
		ST_SetSRID(ST_MakePoint("LONGITUDE", "LATITUDE"), 4326), "CONNECTION_TYPE",
		'iphone' AS device
		FROM temp_""" + simple_sponsor + """iphone"""
		SERVER_INSERT += """ WHERE "SERVER_NAME" LIKE
		"""
		SERVER_INSERT += server_name
		SERVER_INSERT += """ and "TEST_DATE" BETWEEN '""" + str(startdate) + "' and '" + str(enddate) + "'"
		SERVER_INSERT += """
		UNION
		SELECT "CLIENT_IP", "ISP", "TEST_DATE",
		"DOWNLOAD_KBPS", "UPLOAD_KBPS", "LATENCY", "LATITUDE", "LONGITUDE",
		ST_SetSRID(ST_MakePoint("LONGITUDE", "LATITUDE"), 4326), "CONNECTION_TYPE",
		'windows_phone' AS device
		FROM temp_""" + simple_sponsor + """windows_phone"""
		SERVER_INSERT += """ WHERE "SERVER_NAME" LIKE
		"""
		SERVER_INSERT += server_name
		SERVER_INSERT += """ and "TEST_DATE" BETWEEN '""" + str(startdate) + "' and '" + str(enddate) + "'"
		SERVER_INSERT += """ ) mobile
		ORDER BY mobile."TEST_DATE" """
# 	print SERVER_INSERT
	return SERVER_INSERT

table_base = """ (
pk BIGSERIAL PRIMARY KEY,
client_ip varchar,
isp varchar,
test_date timestamp,
download_kbps INTEGER,
upload_kbps INTEGER,
latency INTEGER,
latitude decimal,
longitude decimal,
geom GEOMETRY(POINT, 4326),"""

mobile_table = table_base + """
connection_type VARCHAR,
device VARCHAR)"""

web_table = table_base + """
client_city VARCHAR, 
client_region VARCHAR, 
client_country VARCHAR, 
client_browser VARCHAR, 
client_operating_system VARCHAR, 
user_agent VARCHAR)"""

class Server(object):
	
	def __init__(self, sponsor_string, server_id):
		self.sponsor_string = sponsor_string
		self.server_id = server_id
		self.server_name = self.getServerName()
		
	def getServerName(self):
# 		print self.server_id
		con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password)
		cur = con.cursor()
		cmd = "SELECT name FROM speedtest_servers WHERE server_id = " + str(self.server_id)
# 		print cmd
		cur.execute(cmd)
		for server_name in cur.fetchall():
			return server_name[0]
		
	def insertTests(self, simple_sponsor, startdate, enddate):
		con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password)
		cur = con.cursor()
		for device in ['web_browser', 'mobile']:
			cmd = makeInsert("'" + self.server_name + "'", self.server_id, device, simple_sponsor, startdate, enddate)
			cur.execute(cmd)
			con.commit()
# 			print cur.statusmessage
		con.close()
				
	def dropGeo(self):
		logger = logging.getLogger("pyspeed.Server.dropGeo")
		logger.info("Dropping geo tables for: " + self.sponsor_string)
		con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password)
		cur = con.cursor()
		for device in ['web_browser', 'mobile']:
			if device[0] in 'web_browser':
				cur.execute("DROP TABLE public.geo_" + str(self.server_id) + "_web_browser CASCADE")
				con.commit()
				logger.debug(cur.statusmessage + " " + str(self.server_id) + "_" + device[0])
			else:
				print "DROP TABLE geo_" + str(self.server_id) + "_mobile CASCADE"
				cur.execute("DROP TABLE public.geo_" + str(self.server_id) + "_mobile CASCADE")
				con.commit()
		con.close()
		
	def createGeo(self):
		logger = logging.getLogger("pyspeed.Server.createGeo")
		logger.info("Creating tables for: " + str(self.sponsor_string))
		con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password)
		cur = con.cursor()
		for device in ['web_browser', 'mobile']:
# 			device = device[0]
			if device in 'web_browser':
				cmd = "create table public.geo_"  + str(self.server_id) + "_web_browser" + web_table
# 				print cmd
				cur.execute(cmd)
				logger.debug(cur.statusmessage + " " + str(self.server_id) + "_" + device)
# 				print cur.statusmessage
				con.commit()
			else:
				cmd = "create table public.geo_"  + str(self.server_id) + "_mobile" + mobile_table
# 				print cmd
				cur.execute(cmd)
				logger.debug(cur.statusmessage + " " + str(self.server_id) + "_" + device)
# 				print cur.statusmessage
				con.commit()
# 		con.commit()
		con.close()

	def makeIndex(self):
		INDEX_CMD = """CREATE INDEX ON public.geo_""" + str(self.server_id) + "_"
		con = psycopg2.connect(host=pg_host,database=pg_database, user=pg_user, password=pg_password)
		cur = con.cursor()
		for device in ['web_browser', 'mobile']:
			cmd = INDEX_CMD + device + " (test_date DESC NULLS FIRST);"
			cur.execute(cmd)
		con.commit()
		con.close

