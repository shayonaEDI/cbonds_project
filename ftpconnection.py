#%%
''' 
Host: database.cbonds.info

Login: cbdb_exchange_data

Password: Cbonds20211123

Port: 21'''

import ftplib

HOSTNAME = "database.cbonds.info"
USERNAME = "cbdb_exchange_data"
PASSWORD = "Cbonds20211123"

# Connect FTP Server
ftp_server = ftplib.FTP(HOSTNAME, USERNAME, PASSWORD)
 
# force UTF-8 encoding
ftp_server.encoding = "utf-8"

#%%