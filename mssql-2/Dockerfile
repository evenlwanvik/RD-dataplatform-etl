FROM mcr.microsoft.com/mssql/server:2019-CU5-ubuntu-18.04

ENV SA_PASSWORD=Valhalla06978!
ENV ACCEPT_EULA=Y

USER mssql

COPY init-db.sql /tmp/

# Launch SQL Server, confirm startup is complete, restore the database, then terminate SQL Server.
RUN ( /opt/mssql/bin/sqlservr & ) | grep -q "Service Broker manager has started" \
    && /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P ${SA_PASSWORD} -i /tmp/init-db.sql \
    && pkill sqlservr

CMD ["/opt/mssql/bin/sqlservr"]