#!/bin/bash

/opt/mssql-tools/bin/sqlcmd \
    -l 60 \
    -S localhost -U SA -P "$MSSQL_SA_PASSWORD" &

/opt/mssql/bin/permissions_check.sh "$@"