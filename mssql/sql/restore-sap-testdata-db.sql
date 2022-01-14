RESTORE DATABASE ans_testdata FROM DISK = "/var/opt/mssql/backup/avstemning_sap_data.bak" WITH

MOVE "avstemning_sap_data" to "/var/opt/mssql/data/avstemning_sap_data.mdf", 
MOVE "avstemning_sap_data_Log" to "/var/opt/mssql/data/avstemning_sap_data.ldf", 

NOUNLOAD, STATS = 10