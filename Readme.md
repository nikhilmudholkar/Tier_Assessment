# Documentation

The project contains following folder structure:-


- app.log:- file to store the logs
- config_parser:- class to load the configurations and credentials before executing a script
- cofigurations.yml:- YML file to store the configurations as a key:value pair
- main.py:- Main file to drive the code execution
- Pipfile and Pipfile.lock:- Files to store and lock the packages and their versions used in the code
- requirements.txt:- Stores all the pip packages with their version
- src.LoadJsons.py:- This file contains all the functionality to read jsons and store their data in database
- utils.logger_util.py:- Utility function to setup logger
- utils.postgres_util.py:- Utility function to setup postgres connection

## Things considered while designing the solution:-
- **Data Volume**:- Spark was used instead of using plain python to read and dump the data. Using a for loop to read the json and dumping it into DB will be extremely time consuming and thus not the most optimum solution. Spark is much more efficient and completes the entire process in less than a minute.
- **Data Quality**:- Before duming the data into DB, I have writeen few data quality checks to ensure that only the right data is dumped into the DB. These checks include checks to remove the incorrect data and missing data. I have also written functions to convert the datatype of DateTime columns from text to Datetime. This will make it much easier to query the data and immply time based filters.
- **Scalability**:- The code is highly scalable and can scale up to dump multiple json files instead of just 2. Also spark makes it possible to deal with multiple json files simultaneously as well. This functionality can be leveraged to further increase the scalability.
- **Ease of Deployment**:- The code is written in highly modular manner to ease the deployment process. There are separate directories for utils and src code. THis let's us reuse code and isolate code blocks on the basis of their functionality. Also the main functions are written in a class and this class can be reused for multiple Data. I have used Pipenv to take care of package dependencies and have attached a dockerfile to create docker image.

## Features of the code

- The code uses spark and python pyspark library to do all the data loading, processing
- Configurations are stores in a separate file instead of hard-coding them. This ensures easy management of configurations and secrets
- logging is setup and the logs get's dumped into a file. These logs can also flow into a log analysis platform like splunk for real time monitoring of pipeline
- Proper project structure is maintained with production standards to ensure easy deployment
- Pipfile, Pipfile.lock and requirements.txt is created to ensure correct packages dependency


## Data loading time comparision

the most expensive step is loading the data into the postgres table. I have implemented 3 separate ways to do this and below are the time taken for each method 

| Methods | Time taken for Weather | Time taken for track_events
| ------ | ------ | ------ |
| JDBC Connection | 3.15 seconds | 16.04 seconds |
| JDBC Connections with Repartitions | 1.9 seconds | 22.7 seconds |
| COPY command | 0.14 seconds | 4.64 seconds |


## Scope for improvement

- Currently the best method is copy command. This command runs on several csv files iteratively and pushes the data to Database. This can be further sped up by using multiprocessing to push multiple files simultaneously.
- Currently the data is loaded into spark dataframe using the "spark_context.read.json" method. Spark infers the schema but it scans the entire data first to infer it. This adds to the latency. This read time can therefore be reduced by providing the spark with the schema.

## How to run
- Add both the data jsons in the Data/ folder. 
-  `pipenv install`
- run main.py
