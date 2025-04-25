## Data Integrating from a public REST API for countries information

This project involved integrating data from a public REST API for countries information. This data will be used by a travel agency to recommend travel destinations to customers based on some different factors like language, continents, regions, currency and many more. The world country data is extracted from https://restcountries.com/v3.1/all.
The project is divided into two parts:
- The ETL pipeline orchestrated with airflow, and
- Data visualization with Metabase
All setup is done with Docker using [Docker-compose file](./docker-compose.yaml) for both airflow and Postgres database connection.

In addition to the file, the metabase docker image was pulled from metabase by running
- `docker pull metabase/metabase:latest`
While the metabase container is started by running
- `docker run -d -p 3000:3000 --name metabase metabase/metabase`
To enable a shared network between postgres and metabase
- `docker network connect metabase-network airflow-docker-postgres-1` where airflow-docker-postgres-1 is the postgres container name

### ETL Pipeline
The ETL part of the project is divided into three part:
- Extracted data from the Rest API
- Transformed the data and retrieve fields of interest
- Loaded the transformed data into PostgreSQL database.

The ETL pipeline workflow is orchestrated with airflow. [See DAG file](./dags/country_info_dag.py) for the ETL pipekine code orchestrated in airflow.

### Visualization
Metabase is connected to the PostgreSQL database for visualization and execution of queries. The dashboard visual, which contains the answer to the project questions, can be accessed [here](./WorldCountryDashboard.pdf)

