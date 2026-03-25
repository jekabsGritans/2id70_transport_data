FROM postgres:15

# Install necessary tools for downloading
RUN apt-get update && apt-get install -y curl unzip && rm -rf /var/lib/apt/lists/*

# Copy download script and init SQL
COPY download.sh /docker-entrypoint-initdb.d/01-download.sh
COPY init.sql /docker-entrypoint-initdb.d/02-init.sql

# Make download script executable
RUN chmod +x /docker-entrypoint-initdb.d/01-download.sh

# The download script will run automatically on first container startup
# thanks to docker-entrypoint-initdb.d mechanism
