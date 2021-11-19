# A Study on Foreign Immigrants to The United States 

## Project Summary
---
The objective of this project was to create an ETL pipeline for I94 immigration, US demographics global land temperatures datasets to form an analytics database on immigration events.

A possible use case for this analytics database is to determine various patterns that might occur by joining both the temperature and the demographic dataset together with the US immigration dataset.

This analytics database can be used to find solutions to relevant questions such as:
1. What time of the year does most immigrants come to the US?

2. Do they come due to change in climate of their own country/region?

3. Which race of persons normally immigrate to US?

4. And more.
---

## Data and Code
---
All the data for this project was loaded into S3 prior to commencing the project. The exception is the i94res.csv file which was loaded into Amazon EMR hdfs filesystem.

In addition to the data files, the project workspace includes:

etl.py - reads data from S3, processes that data into fact and dimension tables using Spark, and writes processed data as a set of fact and dimensional tables back to S3

dl.cfg - contains configuration that allows the ETL pipeline to access AWS EMR cluster.

DataQualityCheck.ipynb - Performs the 3 data quality checks

Sandbox.ipynb - test the processing of the data using spark.

etl.ipynb - shows the step by step process of how the data will be processed and parquet back to the file on s3.

DataDictionary.md - Stores the data dictionary for the datasets.

---
The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up
---


## Step 1: Scope the Project and Gather Data
---
#### Scope 

The project will utilize I94 Immigration Data, World Temperature Data and U.S. City Demographic Data to create a data warehouse that will be modeled as a star-schema to optimize performance and provide fast response times.

As such, the data will be stored in both fact and dimension tables. 

The following tools were used to complete this project:

1. Python: Used for data processing
    - Pandas: Exploratory data analysis on small data set
    - PySpark: Data processing on large data set
       
2. AWS S3: Used for data storage

3. AWS Redshift: Used to create the data warehouse and data analysis
---

#### Describe and Gather Data  
---
Data Set|Data Format|Description
--- | --- | ---
**[I94 Immigration Data](https://www.trade.gov/national-travel-and-tourism-office)** | SAS |`This data comes from the US National Tourism and Trade Office. It consists of international visitors arrival statistics, which shows their 4 digit year of birth, gender, airline used to arrive in U.S, visa type, flight number, arrival date to the USA, departure date from the USA, admission number, INS number, port of entry, first state to visit by nationals from selected countries.`
**[U.S. City Demographic Data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/)** | CSV | `This data comes from the US Census Bureau's 2015 American Community Survey. The dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000.` 
**[World Temperature Data](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)** | CSV | `This data come from Kaggle and consists of earth surface temperature that looks into land temperatures by city for each country.`

---
## Step 2: Explore and Assess the Data
---
#### Explore the Data 
---

Identify data quality issues, like missing values, duplicate data, etc.

1. Explore the data using pandas on all three datasets.
2. Split the data set in facts and dimension tables.
3. Rename the columns to make it easier to read.
4. Use PySpark to test the ETL data pipeline logic.

To explore the data, click here: [Explore Data](ExploreData.ipynb)

---

## Step 3: Define the Data Model
---

#### 3.1 Conceptual Data Model
---
To complete this project, we must first determine the type of schema to be uses. The *Star* schema was chosen because of its simplicity. As such, we will have one fact table called Immigrations thats will be centered around 6 dimension tables (Immigrants, Airports, Populations, Population_Statistics, Temperatures and Temperature_Statistics ). We have also created 4 auxiliary tables to get Countries, State, Ports and Visa details.

<div id="91783224923A161706A753E0D5CFB6DF4E6_72950"><div id="91783224923A161706A753E0D5CFB6DF4E6_72950_robot"><a href="https://cloud.smartdraw.com/share.aspx/?pubDocShare=91783224923A161706A753E0D5CFB6DF4E6" target="_blank"><img src="https://cloud.smartdraw.com/cloudstorage/91783224923A161706A753E0D5CFB6DF4E6/preview2.png"></a></div></div><script src="https://cloud.smartdraw.com/plugins/html/js/sdjswidget_html.js" type="text/javascript"></script><script type="text/javascript">SDJS_Widget("91783224923A161706A753E0D5CFB6DF4E6",72950,1,"");</script><br/>

#### 3.2 Mapping Out Data Pipelines
1. Load immigration data from S3 Bucket, Source_S3_Bucket/immigration/18-83510-I94-Data-2016/*.sas7bdat

2. Split immigration into fact (Immigrations) and dimension tables (Immigrants, Airports)
3. Load us-cities-demographics data from S3 Bucket, Source_S3_Bucket/demography/us-cities-demographics.csv
4. Split us-cities-demographics data into dimension tables (Populations, Population_Statistics)
5. Load World Temperature data from S3 Bucket, Source_S3_Bucket/temperature/GlobalLandTemperaturesByCity.csv
6. Split World Temperature data into dimension tables (Temperatures, Temperature_Statistics )
7. Load I94_SAS_Labels_Descriptions.SAS data from S3 Bucket, Source_S3_Bucket/I94_SAS_Labels_Descriptions.SAS
8. Use I94_SAS_Labels_Descriptions.SAS to create auxiliary tables (Countries, State, Ports and Visa)
9. Follow Step 2 – Cleaning step to clean up data sets
10. Store these tables back to target S3 bucket

---

## Step 4: Run Pipelines to Model the Data 
---
#### 4.1 Create the data model
---

To create this model, we used Sandbox to test our structure. To use Sandbox to test the structure of the model, click here: **[Sandbox](Sandbox.ipynb)**

After testing the structure, we then build the data pipelines to create the data model. To build the data pipeline, click here: **[ETL](etl.ipynb)**

---

#### 4.2 Data Quality Checks
---

Data quality checks includes:

1. No empty table after running ETL data pipeline.
2. Data schema of every dimensional table matches data model.
3. Ensure that all Immigration files were added immigration dataframe.

To run the Data Quality Checks, click here: **[Data Quality Check](DataQualityCheck.ipynb)**

---
#### 4.3 Data dictionary 
---

To view data dictionary, click here: **[Data Dictionary](DataDictionary.md)**

---

## Step 5: Complete Project Write Up
---

#### 5.1 Tools and Technologies for the Project
---

Tools & Technologies | Purpose
---|---
**AWS S3** | Used for data storage.
**Pandas with Python** | Used for sample data set exploratory data analysis.
**PySpark with Python** | Used for large data set data processing to transform staging table to fact,dimensional and auxilary tables.

---

#### 5.2 Frequency of Updates
---

- Tables comming from **Immigration Data** can be updated monthly since there is a new file added each month. Thus having the ability to be partitioned monthly.
- **Temperature data** is built to be partitioned monthly. As such, tables coming from **Temperature data** should be updated monthly.
- Tables coming from **Demographic data** should be updated yearly since it was not built to be partitioned monthly.

---

#### 5.3 Potential Upgrades
---

1. **The data was increased by 100x.**
    - If spark has an issue processing data when it is increased by 100x, we can then make use of **[AWS EMR]()**. Apache Spark is linearly scalable, which means you may simply add the number of clusters to increase the performance. With AWS EMR you will be able to adjust the size and number of clusters as you see fit. 

2. **The data populates a dashboard that must be updated on a daily basis by 7am every day.**
    - If we would like to schedule audomated updated daily, we can utilize **[Apache Airfow](https://airflow.apache.org/)**. We could also combine Airflow + Spark + Apache Livy in our EMR cluster so that Spark commands can be passed through an API interface.

3. **The database needed to be accessed by 100+ people.**
    - If we decide to allow 100+ persons to have access to the data, then we can utilize **[AWS Redshift](https://aws.amazon.com/redshift/)** since it allows for up to 500 persons to have access to the data. AWS Redshift has the capability to process a large number of simultaneous queries. Additionally, through AWS IAM features we can manage user access to our clusters. To further improve, we could even use something like Amazon Cognito to manage user accounts since it allows us to create a custom sign-up + login page as well as a user management system.

---
## Additional: Test Data Model
---

For this section, we will create a query from the various Tables that were created for this analytics database.

To view query, click here: [Test Query](TestQueries.ipynb)

---