
# Unit test for connection_setup.py

from connection_setup import get_connection_details, start_ssh_tunnel, connect_database, disconnect_database, close_ssh_tunnel

#get all connection details
ec2_uri, db_uri, ssh_user, ssh_port, remote_db_port, local_forwarding_port, db_username, db_password = get_connection_details()

#create SSH tunnel
ssh_tunnel = start_ssh_tunnel(ec2_uri,db_uri,ssh_user,ssh_port,remote_db_port,local_forwarding_port)

#connect to document DB
client, db = connect_database(db_username, db_password, local_forwarding_port)

#query a sample collection
test_collection = db['raw_material_spec']

for x in test_collection.find():
    print(x)

#close database connection
disconnect_database(client)

#close the SSH tunnel once done
close_ssh_tunnel(ssh_tunnel)
