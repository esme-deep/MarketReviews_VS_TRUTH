# Dockerfile
FROM apache/airflow:3.0.1

USER root

# Installer le pilote ODBC Microsoft
RUN apt-get update && apt-get install -y curl gnupg2 apt-transport-https unixodbc unixodbc-dev libgssapi-krb5-2 \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Installer les dépendances Python
USER airflow
RUN pip install --no-cache-dir \
    pyodbc \
    polars \
    python-dotenv \
    requests \
    beautifulsoup4