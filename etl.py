# import files
import configparser
from datetime import datetime
import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import udf, col, lit, year, month, upper, to_date
from pyspark.sql.functions import monotonically_increasing_id
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date


#Load cinfigerations
config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
        creating the session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()
    return spark


def Convert_SAS_to_date(date):
        if date is not None:
            return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')
    
Convert_SAS_to_date_udf = udf(Convert_SAS_to_date, DateType())


#Function to get all filepaths from repository ending with '.sas7bdat'
def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.sas7bdat'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files


def process_immigration_data(spark, input_data, output_data):
    """
    Loads immigration data from s3
    and proccess data to get fact and dimension tables.
    
    Parameters
    ----------
    spark : SparkSession
        Apache Spark session
    input_data : str
        The path prefix for the immigration-data.
    output_data : str
        The path prefix for the output data.
    
    """
    
    # get filepath to immigration data files
    filepath = input_data + '../../data/18-83510-I94-Data-2016/'
    all_files = get_files(filepath)
    
    #create immigration-data schema
    immigrationSchema = R([
        Fld("cicid",Int()),
        Fld("i94yr",Int()),
        Fld("i94mon",Int()),
        Fld("i94cit",Int()),
        Fld("i94res",Int()),
        Fld("i94port",Str()),
        Fld("arrdate",Dbl()),
        Fld("i94mode",Int()),
        Fld("i94addr",Str()),
        Fld("depdate",Dbl()),
        Fld("i94bir",Int()),
        Fld("i94visa",Int()),
        Fld("count",Int()),
        Fld("dtadfile",Str()),
        Fld("visapost",Str()),
        Fld("occup",Str()),
        Fld("entdepa",Str()),
        Fld("entdepd",Str()),
        Fld("entdepu",Str()),
        Fld("matflag",Str()),
        Fld("biryear",Int()),
        Fld("dtaddto",Str()),
        Fld("gender",Str()),
        Fld("insnum",Str()),
        Fld("airline",Str()),
        Fld("admnum",DecimalType(18, 0)),
        Fld("fltno",Str()),
        Fld("visatype",Str())
    ])
    
    
    # Create an empty RDD
    emp_RDD = spark.sparkContext.emptyRDD()

    # Create an empty RDD with expected schema
    df_immigration = spark.createDataFrame(data = emp_RDD,schema = immigrationSchema)
    
    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
            # read files in immigration data files
            df_temp = spark.read.format('com.github.saurfang.sas.spark').load(datafile)
            df_temp = df_temp\
                            .withColumn('cicid', col('cicid').cast(IntegerType()))\
                            .withColumn('i94yr', col('i94yr').cast(IntegerType()))\
                            .withColumn('i94mon', col('i94mon').cast(IntegerType()))\
                            .withColumn('i94cit', col('i94cit').cast(IntegerType()))\
                            .withColumn('i94res', col('i94res').cast(IntegerType()))\
                            .withColumn('i94mode', col('i94mode').cast(IntegerType()))\
                            .withColumn('i94visa', col('i94visa').cast(IntegerType()))\
                            .withColumn('count', col('count').cast(IntegerType()))\
                            .withColumn('biryear', col('biryear').cast(IntegerType()))\
                            .withColumn('i94bir', col('i94bir').cast(IntegerType()))\
                            .withColumn('admnum', col('admnum').cast(DecimalType(18, 0)))
        
            if datafile == all_files[4]:
                df_temp = df_temp\
                                .drop('validres','delete_days','delete_mexl','delete_dup','delete_visa','delete_recdup')
            df_immigration = df_immigration.union(df_temp)  
            
            
    # Create Fact Table: Immigrations
    Immigrations = df_immigration.select('cicid', 'i94yr', 'i94mon','i94addr', 'i94port', 'i94mode','i94visa', 'arrdate', 'depdate' ,'matflag')\
                    .distinct()\
                    .withColumn("immigration_id", monotonically_increasing_id())
            
            
    # Rename Columns in Fact Table
    new_column_name = ['cic_id', 'year', 'month', 'state_code','port_code', 'mode_code','visa_code',\
                                   'arrival_date','departure_date','match_flag','immigration_id']
    Immigrations = Immigrations.toDF(*new_column_name)
    
    # convert both arrival and departure date to date format
    Immigrations = Immigrations.withColumn('arrival_date',Convert_SAS_to_date_udf(Immigrations['arrival_date']))
    Immigrations = Immigrations.withColumn('departure_date',Convert_SAS_to_date_udf(Immigrations['departure_date']))
    
    # write Immigrations table to parquet files partitioned by state_code
    Immigrations.write.mode('overwrite').partitionBy("state_code").parquet(output_data+'Immigrations/')
    
    
    #Create 1st Dimension Table: Immigrants
    Immigrants = df_immigration.select('cicid', 'i94cit', 'i94res', 'i94bir', 'gender', 'insnum')\
                     .distinct()\
                     .withColumn("immigrants_id", monotonically_increasing_id())
    
    
    # Rename Columns for 1st Dimension Table: Immigrants
    new_column_name = ['cic_id','citizen_country', 'residence_country','age','gender','ins_num','immigrants_id']
    Immigrants = Immigrants.toDF(*new_column_name)
    
    
    # write Immigrants table to parquet files 
    Immigrants.write.mode('overwrite').parquet(output_data+'Immigrants/')
    
    
    
    #Create 2nd Dimension Table: Airports
    Airports = df_immigration.select('cicid','airline','fltno','admnum','visatype')\
                    .distinct()\
                    .withColumn("Airports_id", monotonically_increasing_id())
    
    
    #Rename Columns 2nd Dimension Table: Airports
    new_column_name = ['cic_id','airline','flight_number','admin_number','visa_type','Airports_id']
    Airports = Airports.toDF(*new_column_name)
    
    # write Immigrants table to parquet files 
    Airports.write.mode('overwrite').parquet(output_data+'Airports/')

    
def process_demographic_data(spark, input_data, output_data):
    """
    Loads demographic data from s3
    and proccess data to get dimension tables.
    
    Parameters
    ----------
    spark : SparkSession
        Apache Spark session
    input_data : str
        The path prefix for the demographic-data.
    output_data : str
        The path prefix for the output data.
    
    """
    # get filepath to demographic data file
    demographic_data = input_data + "us-cities-demographics.csv"
    

     # read demographic data file
    df_demographic = spark.read.format('csv').load(demographic_data,delimiter=';',header=True)
    df_demographic = df_demographic\
                    .withColumn('City',upper(df_demographic['city']))\
                    .withColumn('State',upper(df_demographic['State']))\
                    .withColumn('Male Population', col('Male Population').cast(IntegerType()))\
                    .withColumn('Female Population', col('Female Population').cast(IntegerType()))\
                    .withColumn('Total Population', col('Total Population').cast(IntegerType()))\
                    .withColumn('Number of Veterans', col('Number of Veterans').cast(IntegerType()))\
                    .withColumn('Foreign-born', col('Foreign-born').cast(IntegerType()))\
                    .withColumn('Median Age', col('Median Age').cast(DoubleType()))\
                    .withColumn('Average Household Size', col('Average Household Size').cast(DoubleType()))
    
    
    #Create Dimension Table: Populations
    Populations = df_demographic.select('City','State','State Code','Male Population','Female Population',\
                                        'Total Population','Number of Veterans','Foreign-born','Race')\
                    .distinct()\
                    .withColumn("population_id", monotonically_increasing_id())
    
    #Rename Columns for Dimension Table: Populations
    new_column_name =['city','state','state_code','male_population','female_population','total_population'\
                      ,'num_of_veterans','foreign_born','race','population_id']
    Populations = Populations.toDF(*new_column_name)
    
    # write Populations table to parquet files partitioned by state
    Populations.write.mode('overwrite').partitionBy("state").parquet(output_data+'Populations/')
    
    
    #Create 2nd Dimension Table: Population_Statistics
    Population_Statistics = df_demographic.select('City','State','State Code','Median Age','Average Household Size')\
                    .distinct()\
                    .withColumn("pop_statistics_id", monotonically_increasing_id())
    
    #Rename Columns for 2nd Dimension Table: Population_Statistics
    new_column_name = ['city','state','state_code','median_age','avg_household_size','pop_statistics_id']
    Population_Statistics =  Population_Statistics.toDF(*new_column_name)
    
    # write Population_Statistics table to parquet files partitioned by state
    Population_Statistics.write.mode('overwrite').partitionBy("state").parquet(output_data+'Population_Statistics/')
    

    
    
def process_temperature_data(spark, input_data, output_data):
    """
    Loads temperature data from s3
    and proccess data to get dimension tables.
    
    Parameters
    ----------
    spark : SparkSession
        Apache Spark session
    input_data : str
        The path prefix for the temperature-data.
    output_data : str
        The path prefix for the output data.
    
    """
    # get filepath to temperature data file
    temperature_data = input_data + "../../data2/*.csv"
    
    
     # read temperature data file
    df_temperature = spark.read.format('csv').load(temperature_data,header=True)
    df_temperature = df_temperature\
                        .withColumn('AverageTemperature', col('AverageTemperature').cast(DoubleType()))\
                        .withColumn('AverageTemperatureUncertainty', col('AverageTemperatureUncertainty').cast(DoubleType()))\
                        .withColumn('City',upper(df_temperature['City']))\
                        .withColumn('Country',upper(df_temperature['Country']))
    
    
    
    # Add year and month to temperature data
    df_temperature = df_temperature.withColumn('dt', to_date(df_temperature['dt']))
    df_temperature = df_temperature.withColumn('year', year(df_temperature['dt']))
    df_temperature = df_temperature.withColumn('month', month(df_temperature['dt']))
    
    
       
    
    #Create Dimension Table: Temperatures
    Temperatures = df_temperature.select('dt','year','month','Country','City','Latitude','Longitude').distinct()
    
    #Rename Columns
    new_column_name =['date','year','month','country','city','latitude','longitude']
    Temperatures = Temperatures.toDF(*new_column_name)
    
    # write Temperatures table to parquet files 
    Temperatures.write.mode('overwrite').parquet(output_data+'Temperatures/')
    
    
    #Create Dimension Table Temperature_Statistics
    Temperature_Statistics = df_temperature.select('dt','year','month','Country','City'\
                                                       ,'AverageTemperature','AverageTemperatureUncertainty')
    
    
    #Rename Columns
    new_column_name =['date','year','month','country','city','avg_temp','avg_temp_uncertainty']
    Temperature_Statistics = Temperature_Statistics.toDF(*new_column_name)
    
    
    # write Temperatures table to parquet files 
    Temperature_Statistics.write.mode('overwrite').parquet(output_data+'Temperature_Statistics/')
    
    
def process_libray_data(spark, input_data, output_data):
    """
    Loads libray data from s3
    and proccess data to get dimension tables.
    
    Parameters
    ----------
    spark : SparkSession
        Apache Spark session
    input_data : str
        The path prefix for the libray-data.
    output_data : str
        The path prefix for the output data.
    
    """
    # get filepath to library data file
    library_data = input_data + "I94_SAS_Labels_Descriptions.SAS"
    
    
     # read library data file
    with open(library_data) as library:
        lines = library.readlines()
    
    #Create Countries Auxilary table
    
    # Create Auxilary Table: Countries
    # Create an empty RDD
    emp_RDD = spark.sparkContext.emptyRDD()

    # Create an expected schema
    columns = StructType([StructField('code',StringType(), True)\
                    ,StructField('country',StringType(), True)])
    # Create an empty RDD with expected schema
    Countries = spark.createDataFrame(data = emp_RDD,
                           schema = columns)
    
    # Insert into Auxilary Table: Countries
    country_data = lines[9:298]
    for data in country_data:
        temp = data.split('=')
        list = [temp[0].strip(), temp[1].strip().strip("'").strip(";")]
        columns =  ['code', 'country']
        newRow = spark.createDataFrame([list], columns)
        Countries = Countries.union(newRow)
    
    # Convert code datatype to int
    Countries = Countries.withColumn('code', col('code').cast(IntegerType()))
    
    # write Countries Auxilary Table to parquet files 
    Countries.write.mode('overwrite').parquet(output_data+'Countries/')
    
        
    # Create Auxilary Table: States
    # Create an empty RDD
    emp_RDD = spark.sparkContext.emptyRDD()

    # Create an expected schema
    columns = StructType([StructField('state_code',StringType(), True)\
                    ,StructField('state',StringType(), True)])
    # Create an empty RDD with expected schema
    States = spark.createDataFrame(data = emp_RDD,schema = columns)
    
    # Insert into Auxilary Table: States
    country_data = lines[981:1036]
    for data in country_data:
        temp = data.split('=')
        list = [temp[0].strip(), temp[1].strip().strip("'").strip(";")]
        columns =  ['state_code', 'state']
        newRow = spark.createDataFrame([list], columns)
        States = States.union(newRow)
        
    # write States Auxilary Table to parquet files 
    States.write.mode('overwrite').parquet(output_data+'States/')    
    
    # Create Auxilary Table: Ports
    # Create an empty RDD
    emp_RDD = spark.sparkContext.emptyRDD()

    # Create an expected schema
    columns = StructType([StructField('port_code',StringType(), True)\
                    ,StructField('port_city',StringType(), True)])
    # Create an empty RDD with expected schema
    Ports = spark.createDataFrame(data = emp_RDD,schema = columns)
    
    # Insert into Auxilary Table: Ports
    country_data = lines[302:961]
    for data in country_data:
        temp = data.split('=')
        list = [temp[0].strip(), temp[1].strip().strip("'").split(',')[0]]
        columns =  ['port_code', 'port_city']
        newRow = spark.createDataFrame([list], columns)
        Ports = Ports.union(newRow)
        
    # write Ports Auxilary Table to parquet files 
    Ports.write.mode('overwrite').parquet(output_data+'Ports/')
    
        
    # Create Auxilary Table: Visas
    # Create an empty RDD
    emp_RDD = spark.sparkContext.emptyRDD()

    # Create an expected schema
    columns = StructType([StructField('visa_code',StringType(), True)\
                    ,StructField('visa',StringType(), True)])

    # Create an empty RDD with expected schema
    Visas = spark.createDataFrame(data = emp_RDD,schema = columns)
    
    # Insert into Auxilary Table: Visas
    country_data = lines[1046:1049]
    for data in country_data:
        temp = data.split('=')
        list = [temp[0].strip(), temp[1].strip().strip("'").strip(";")]
        columns =  ['visa_code', 'visa']
        newRow = spark.createDataFrame([list], columns)
        Visas = Visas.union(newRow)
    
    # Convert visa_code datatype to int
    Visas = Visas.withColumn('visa_code', col('visa_code').cast(IntegerType()))
    
    # write Visas Auxilary Table to parquet files 
    Visas.write.mode('overwrite').parquet(output_data+'Visas/')
    
def main():
    spark = create_spark_session()
    input_data = ""
    
    output_data = "Capstone_Project/"
    
    
    process_demographic_data(spark, input_data, output_data)
    process_temperature_data(spark, input_data, output_data)
    process_libray_data(spark, input_data, output_data)
    process_immigration_data(spark, input_data, output_data)
    
if __name__ == "__main__":
    main()