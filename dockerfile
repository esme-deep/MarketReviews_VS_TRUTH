# Dockerfile
FROM apache/airflow:3.0.1

USER root

# Installer le pilote ODBC Microsoft 18
# Nettoyé pour enlever les espaces insécables et optimisé
RUN apt-get update && \
    apt-get install -y curl gnupg2 apt-transport-https unixodbc unixodbc-dev libgssapi-krb5-2 && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    # Nettoie le cache apt pour garder l'image légère
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Installer les dépendances Python
USER airflow
RUN pip install --no-cache-dir \
    pyodbc \
    python-dotenv \
    requests \
    beautifulsoup4 \
    torch \
    transformers \
    scipy \
    praw