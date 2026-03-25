FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    curl git sqlite3 unzip \
    && rm -rf /var/lib/apt/lists/*

RUN pip install logica

WORKDIR /workspace
COPY requirements.txt .
RUN pip install -r requirements.txt