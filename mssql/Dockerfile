FROM mcr.microsoft.com/mssql/server:2019-latest

ENV MSSQL_SA_PASSWORD=Valhalla06978!
ENV ACCEPT_EULA=Y
USER root

COPY restore-db.sh entrypoint.sh /opt/mssql/bin/
RUN chmod +x /opt/mssql/bin/restore-db.sh /opt/mssql/bin/entrypoint.sh

COPY AdventureWorksLT2019.bak /var/opt/mssql/data/AdventureWorksLT2019.bak
RUN chown -R mssql:root /var/opt/mssql/data && \
                chmod 0755 /var/opt/mssql/data && \
                chmod -R 0650 /var/opt/mssql/data/*

USER mssql
RUN /opt/mssql/bin/restore-db.sh
CMD [ "/opt/mssql/bin/sqlservr" ]
ENTRYPOINT [ "/opt/mssql/bin/entrypoint.sh" ]