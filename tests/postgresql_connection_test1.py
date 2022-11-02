
# Unit test for postgresql connection_setup.py
import sys

sys.path.append("../relic_gemd/common/postgresql")
from connection_setup import get_connection_details, connect_local_database, disconnect_local_database

#get all connection details
db_name,uid,pwd,server,port = get_connection_details()

#connect to local DB
connection = connect_local_database(db_name,uid,pwd,server,port)

#run one of the DB functions
cursor = connection.execute("select * from datamigration.generate_measurement_templates()")
row = cursor.fetchone()
print(row)
cursor.close()
