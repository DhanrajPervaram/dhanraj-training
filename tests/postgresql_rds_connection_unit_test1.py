
# Unit test for postgresql connection_setup.py
import sys

from connection_setup import get_connection_details, start_ssh_tunnel, connect_database, disconnect_database

#get all connection details
target_db,aws_region,ec2_uri,ssh_key,ssh_user,ssh_port,remote_db_port,local_forwarding_port,rds_endpoint,rds_db,rds_db_username,rds_db_password,rds_secret_id = get_connection_details()

#create SSH tunnel
ssh_tunnel = start_ssh_tunnel(ec2_uri,ssh_key,rds_endpoint,ssh_user,ssh_port,remote_db_port,local_forwarding_port)

print(rds_db_password)
#print(rds_secret_id)
#print(ssh_key)
#connect to RDS
connection = connect_database(target_db,aws_region,rds_secret_id,rds_db,rds_db_username,rds_db_password,rds_endpoint,local_forwarding_port)

#run one of the DB functions
cursor = connection.execute("select * from lab")
rows = cursor.fetchall()
for row in rows:
   print(row)
cursor.close()
