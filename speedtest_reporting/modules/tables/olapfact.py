from helpers.postgres import commitCommand

def makeSelectCMD(server_id):
	cmd="""SELECT
CONCAT(pk, '-', """ + str(server_id)+""") AS uid,
""" + str(server_id) + """ AS server_id, pk,
download_kbps, upload_kbps, latency,
date(test_date) AS date
FROM
public.geo_""" + str(server_id) + """_mobile
"""
	return cmd


def makeTableCMD(sponsors):
	cmd = "CREATE TABLE olap.fact AS "
	for sponsor in sponsors[:-1]:
		for server_id in sponsor.getServerIDs():
			cmd += makeSelectCMD(server_id) + """
			UNION ALL
			"""
	for server_id in sponsors[-1].getServerIDs():
		print server_id
		cmd += makeSelectCMD(server_id)
	print cmd
	return cmd

def makeIndexesCMD():
	index_cmd = """CREATE INDEX ON olap.fact (date);"""
	return index_cmd

def makeFactTable(sponsors):
	commitCommand("DROP TABLE olap.fact")
	commitCommand(makeTableCMD(sponsors))
	commitCommand(makeIndexesCMD())