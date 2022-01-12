FROM mcr.microsoft.com/mssql/server:2019-CU5-ubuntu-18.04

ENV SA_PASSWORD=Valhalla06978!
ENV ACCEPT_EULA=Y

USER mssql

COPY restore-adventureworks-db.sql restore-ans-testdata-db.sql /tmp/
COPY AdventureWorks2019.bak ans_testdata-2022112-10-35-22.bak /var/opt/mssql/backup/

# Launch SQL Server, confirm startup is complete, restore the database, then terminate SQL Server.
RUN ( /opt/mssql/bin/sqlservr & ) | grep -q "Service Broker manager has started" \
    && /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P ${SA_PASSWORD} -i /tmp/restore-adventureworks-db.sql \
    && /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P ${SA_PASSWORD} -i /tmp/restore-ans-testdata-db.sql \
    && pkill sqlservr

CMD ["/opt/mssql/bin/sqlservr"]