FROM postgres:15

COPY init.sql /docker-entrypoint-initdb.d/01-init.sql
COPY create_edges.sql /docker-entrypoint-initdb.d/02-create_edges.sql