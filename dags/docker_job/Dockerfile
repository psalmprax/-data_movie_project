# Use an official Python runtime as a parent image
FROM apache/airflow:2.7.3

# Set environment variables for ODBC Driver installation
ENV ACCEPT_EULA=Y
ENV MSSQL_DEB_URL=https://packages.microsoft.com/debian/9/prod/pool/main/m/msodbcsql17
ENV MSSQL_RELEASE=17.9.1.1-1

# Set environment variables for dbt
ENV DBT_PROFILE=my_data_quality_profile
ENV DBT_TARGET_DATABASE=my_target_database
ENV DBT_TARGET_SCHEMA=my_target_schema

# Set environment variables for Airflow
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

USER root

# Install ODBC driver dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
       gnupg \
       curl \
       libc6 \
       unixodbc \
       unixodbc-dev

# # Install ODBC Driver for SQL Server
RUN TERM=xterm DEBIAN_FRONTEND=noninteractive apt-get update \
    && apt-get install -y --no-install-recommends \
       gnupg \
       curl \
       libc6 \
       unixodbc \
       unixodbc-dev \
    && curl $MSSQL_DEB_URL/msodbcsql17_${MSSQL_RELEASE}_amd64.deb -o msodbcsql.deb \
    && dpkg -i msodbcsql.deb \
    && rm msodbcsql.deb \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set environment variable for library path
ENV LD_LIBRARY_PATH=/opt/microsoft/msodbcsql17/lib64:$LD_LIBRARY_PATH

# Install ODBC driver dependencies
RUN apt-get update && \
    apt-get install -y unixodbc-dev
RUN apt-get update \
    && apt-get install -y wget zip unzip awscli xvfb \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo 'deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main' | tee /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    # && apt-get install -y google-chrome-stable \
    && apt-get -fy install \
    && apt --fix-broken install \
    && chmod -R 777 /opt/airflow/logs /opt/airflow/dags /opt/airflow \
    && chmod -R 777 * \
    && chown -R airflow:5000 /opt/airflow/logs /opt/airflow /opt/airflow/dags

USER airflow
# Install required Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    && pip install 'apache-airflow[amazon, prometheus]' \
    && pip install 'apache-airflow-providers-amazon' \
    && eval `ssh-agent -s`

RUN airflow db migrate && airflow users create --username airflow --firstname airflow --lastname airflow --email airflow@airflow.com --password airflow --role Admin