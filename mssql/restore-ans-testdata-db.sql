RESTORE DATABASE ans_testdata FROM DISK = "/var/opt/mssql/backup/ans_testdata-2022112-10-35-22.bak" WITH

MOVE "ans_testdata" to "/var/opt/mssql/data/ans_testdata.mdf", 
MOVE "ans_testdata_Log" to "/var/opt/mssql/data/ans_testdata_log.ldf", 

NOUNLOAD, STATS = 5