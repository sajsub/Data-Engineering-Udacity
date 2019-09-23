Data Pipelines with Airflow
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of CSV logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

Project Setup - to run locally
Install Airflow, create variable AIRFLOW_HOME and AIRFLOW_CONFIG with the appropiate paths, and place dags and plugins on airflor_home directory.
Initialize Airflow data base with airflow initdb, and open webserver with airflow webserver
Access the server http://localhost:8080 and create:
AWS Connection Conn Id: Enter aws_credentials. Conn Type: Enter Amazon Web Services. Login: Enter your Access key ID from the IAM User credentials you downloaded earlier. Password: Enter your Secret access key from the IAM User credentials you downloaded earlier.

Redshift Connection Conn Id: Enter redshift. Conn Type: Enter Postgres. Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. Schema: This is the Redshift database you want to connect to. Login: Enter awsuser. Password: Enter the password created when launching the Redshift cluster. Port: Enter 5439.

Project Strucute
README: Current file, holds instructions and documentation of the project
dags/sparkify_dend_dag.py: Directed Acyclic Graph definition with imports, tasks and task dependencies
dags/sparkify_dend_dimensions_subdag.py: SubDag containing loading of Dimensional tables tasks
plugins/helpers/sql_queries.py: Contains Insert SQL statements
plugins/operators/create_tables.sql: Contains SQL Table creations statements
plugins/operators/create_tables.py: Operator that creates Staging, Fact and Dimentional tables
plugins/operators/stage_redshift.py: Operator that copies data from S3 buckets into redshift staging tables
plugins/operators/load_dimension.py: Operator that loads data from redshift staging tables into dimensional tables
plugins/operators/load_fact.py: Operator that loads data from redshift staging tables into fact table
plugins/operators/data_quality.py: Operator that validates data quality in redshift tables
