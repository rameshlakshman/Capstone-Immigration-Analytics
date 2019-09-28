# Import the necessary packages and modules here
import pandas as pd
import re
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType

def create_sparksess_sqlctx():
    #Creating sparksession here, and setting up the sqlContext    
    spark = SparkSession\
    .builder \
    .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
    .appName("Capstone-final-proj")\
    .enableHiveSupport().getOrCreate()

    sqlContext = SQLContext(spark)
    
    return spark, sqlContext

def read_lookups():
    """ Purpose of this function is to read below files, and save it to a dictionary object
            valid_port_i94.txt    - contains all city ports info
            city_codes.txt        - contains all city code abbreviations
            state_codes.txt       - contains all state code abbreviations
            country_codes.txt     - contains all country code abbreviations
    """
    # Dictionary for i94ports
    re_obj = re.compile(r'\'(.*)\'.*\'(.*)\'')    
    valid_port_i94 = {}
    with open('valid_port_i94.txt') as f:
        for line in f:
            cont = re_obj.search(line)
            valid_port_i94[cont[1]]=cont[2]

    # Dictionary for country codes            
    re_obj1 = re.compile(r'\'(.*)\'.*\'(.*)\'')     
    country_codes = {}
    with open('country_codes.txt') as f:
        for line in f:
            cont1 = re_obj1.search(line)
            country_codes[cont1[1]]=cont1[2]

    # Dictionary for state codes            
    re_obj2 = re.compile(r'\'(.*)\'.*\'(.*)\'')     
    state_codes = {}
    state_codes_rev = {}
    with open('state_codes.txt') as f:
        for line in f:
            cont2 = re_obj2.search(line)
            state_codes[cont2[1]]=cont2[2]
            state_codes_rev[cont2[2]]=cont2[1]

    # Dictionary for city codes            
    re_obj3 = re.compile(r'\'(.*)\'.*\'(.*)\'')        
    city_codes = {}
    with open('city_codes.txt') as f:
        for line in f:
            cont3 = re_obj3.search(line)
            city_codes[cont3[1]]=cont3[2]
    
    #return all dictionaries created to invoked object
    return valid_port_i94, country_codes, state_codes, state_codes_rev, city_codes

def read_data_files(spark):
    """ Purpose of this function is to read all the four data files being used 
        in the processing, into the SPARK object. 
        Files are ==> i94 data,   demography data,   airport data,  temperature data
    """
    i94_file_data=spark.read.format('com.github.saurfang.sas.spark').load("../../data/18-83510-I94-Data-2016/i94_jun16_sub.sas7bdat")
    demo_file_data=spark.read.format("csv").option("header", "true").option("delimiter", ";").load("us-cities-demographics.csv")
    airport_file_data=spark.read.format("csv").option("header", "true").load("airport-codes_csv.csv")
    temp_file_data=spark.read.format("csv").option("header", "true").load("GlobalLandTemperaturesByState.csv")
    
    return i94_file_data, demo_file_data, airport_file_data, temp_file_data

def demograph_proc(demo_file_data):
    """ Purpose of this function is to process spark-dataframe that has demography data
        and perofrm necessary cleansing and transformation of the data
    """
    #Percentage calc of columns & add to new cols.
    demographics_data=demo_file_data\
    .withColumn("Median Age",col("Median Age").cast("float"))\
    .withColumn("percent_male_population",demo_file_data["Male Population"]/demo_file_data["Total Population"]*100)\
    .withColumn("percent_female_population",demo_file_data["Female Population"]/demo_file_data["Total Population"]*100)\
    .withColumn("percent_veterans",demo_file_data["Number of Veterans"]/demo_file_data["Total Population"]*100)\
    .withColumn("percent_foreign_born",demo_file_data["Foreign-born"]/demo_file_data["Total Population"]*100)\
    .withColumn("percent_race",demo_file_data["Count"]/demo_file_data["Total Population"]*100)\
    .orderBy("State")

    #New dataframe wth state codes & newly calculated percentages.
    new_demographics_data=demographics_data.select("State",col("State Code").alias("state_code"),\
                                                   col("Median Age").alias("median_age"),\
                                                   "percent_male_population",\
                                                   "percent_female_population",\
                                                   "percent_veterans",\
                                                   "percent_foreign_born",\
                                                   "Race",\
                                                   "percent_race")

    #pivot Race column
    pivot_demographics_data=new_demographics_data.groupBy("State","state_code","median_age","percent_male_population",\
                                                          "percent_female_population","percent_veterans",\
                                                          "percent_foreign_born").pivot("Race").avg("percent_race")

    #header formatting
    pivot_demographics_data=pivot_demographics_data.select("State","state_code","median_age","percent_male_population",\
                                                           "percent_female_population","percent_veterans","percent_foreign_born",\
                                                           col("American Indian and Alaska Native").alias("native_american"),\
                                                           col("Asian").alias("asian"),\
                                                           col("Black or African-American").alias("black"),\
                                                           col("Hispanic or Latino").alias("hispanic_or_latino"),\
                                                           col("White").alias("white"))

    #calc avg of each column by state
    pivot_state=pivot_demographics_data.groupBy("State","state_code").avg("median_age",\
                                                                          "percent_male_population","percent_female_population",\
                                                                          "percent_veterans","percent_foreign_born","native_american",\
                                                                          "asian","black","hispanic_or_latino","white").orderBy("State")

    #Round percentage attributes & rename them
    final_demographics_data=pivot_state.select("State","state_code",round(col("avg(median_age)"),1).alias("median_age"),\
                                               round(col("avg(percent_male_population)"),1).alias("percent_male_population"),\
                                               round(col("avg(percent_female_population)"),1).alias("percent_female_population"),\
                                               round(col("avg(percent_veterans)"),1).alias("percent_veterans"),\
                                               round(col("avg(percent_foreign_born)"),1).alias("percent_foreign_born"),\
                                               round(col("avg(native_american)"),1).alias("native_american"),\
                                               round(col("avg(asian)"),1).alias("asian"),\
                                               round(col("avg(hispanic_or_latino)"),1).alias("hispanic_or_latino"),\
                                               round(col("avg(black)"),1).alias("black"),\
                                               round(col('avg(white)'),1).alias('white')
                                              )

    print('Exiting demograph_proc' )
    return final_demographics_data

def temperature_proc(temp_file_data, state_codes_rev, state_udf_rev):
    """ Purpose of this function is to process temperature file data content
        and perform necessary cleansing and transformation
    """
    #filter temperature data for U.S. and year 2013 (for latest data)
    #add year, month columns,  ##add fahrenheit column,  ###add state abbrevation,  ####drop dups
    temperature_data=temp_file_data.filter(temp_file_data["country"]=="United States")\
    .filter(year(temp_file_data["dt"])==2013)\
    .filter(upper(col("State")).isin(list(state_codes_rev.keys())))\
    .withColumn("year",year(temp_file_data["dt"]))\
    .withColumn("month",month(temp_file_data["dt"]))\
    .withColumn("avg_temp_fahrenheit",temp_file_data["AverageTemperature"]*9/5+32)\
    .withColumn("state_abbreviations",state_udf_rev(upper(temp_file_data["State"])))

    final_temperature_data=temperature_data.select("year","month",round(col("AverageTemperature"),1).alias("avg_temp_celcius"),\
                                           round(col("avg_temp_fahrenheit"),1).alias("avg_temp_fahrenheit"),\
                                           "state_abbreviations","State","Country").dropDuplicates()

    print('Exiting temperature_proc')
    return final_temperature_data

def airports_proc(airport_codes):
    """ Purpose of this function is to process temperature file data content
        and perform necessary cleansing and transformation
    """
    #Filter 'small_airport' in U.S. & substring to extract state
    airport_data=airport_codes.filter(airport_codes["type"]=="small_airport")\
    .filter(airport_codes["iso_country"]=="US")\
    .withColumn("iso_region",substring(airport_codes["iso_region"],4,2))\
    .withColumn("elevation_ft",col("elevation_ft").cast("float"))

    #calc avg elevation by state
    airport_elevation=airport_data.groupBy("iso_country","iso_region").avg("elevation_ft")

    #sel attributes & drop dups
    final_airport_data=airport_elevation.select(col("iso_country").alias("country"),\
                                                   col("iso_region").alias("state"),\
                                                   round(col("avg(elevation_ft)"),1).alias("avg_elevation_ft")).orderBy("iso_region")
    print('Exiting airports_proc')
    return final_airport_data

def immidata_proc(i94_file_data, state_codes, city_codes, country_udf, state_udf, city_udf):
    """ Purpose of this function is to process IMMIGRATION file data content
        and perform necessary cleansing and transformation
    """
    # remove nulls frm i94addr & i94res  ## filter for state, city in list
    ### convert i94res & i94addr columns using respective UDF functions
    i94_data=i94_file_data.filter(i94_file_data.i94addr.isNotNull())\
    .filter(i94_file_data.i94res.isNotNull())\
    .filter(col("i94addr").isin(list(state_codes.keys())))\
    .filter(col("i94port").isin(list(city_codes.keys())))\
    .withColumn("origin_country",country_udf(i94_file_data["i94res"].cast('integer').cast('string')))\
    .withColumn("dest_state_name",state_udf(i94_file_data["i94addr"]))\
    .withColumn("i94yr",col("i94yr").cast("integer"))\
    .withColumn("i94mon",col("i94mon").cast("integer"))\
    .withColumn("city_port_name",city_udf(i94_file_data["i94port"]))

    final_i94_data=i94_data.select("cicid",col("i94yr").alias("year"),col("i94mon").alias("month"),\
                                 "origin_country","i94port","city_port_name",col("i94addr").alias("state_code"),"dest_state_name")

    print('Exiting immidata_proc')
    return final_i94_data

def create_dim_tables(final_demographics_data, final_temperature_data ,final_i94_data, final_airport_data, sqlContext):
    """ Purpose of this function is to create DIMENSION tables from
        dataframes created after necessary cleansing/transformation
    """
    temperature = final_temperature_data.createOrReplaceTempView("temperature")
    print('Temperature Table Successfully created')
    immigration = final_i94_data.createOrReplaceTempView("immigration")
    print('Immigration Table Successfully created')
    demographics = final_demographics_data.createOrReplaceTempView("demographics")
    print('Demographics Table Successfully created')
    airport = final_airport_data.createOrReplaceTempView("airport")
    print('Airport Table Successfully created')

    #Grant unlimited time for joins & parquet writes
    sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "0")

    print('Exiting create_dim_tables')    
    return sqlContext, temperature, immigration, demographics, airport

def build_fct_tbl(temperature, immigration, demographics, airport, spark):
    """ Purpose of this function is to build FACT table from
        dimension tables created in previous step, and tells us
        how many people immigrated to each state in USA
        
        And write dataframe to PARQUET file
    """    
    immigration_to_states=spark.sql("""SELECT
                                        m.year,
                                        m.month AS immigration_month,
                                        m.origin_country AS immigration_origin,
                                        m.dest_state_name AS to_immigration_state,
                                        COUNT(m.state_code) AS to_immigration_state_count,
                                        t.avg_temp_fahrenheit,
                                        a.avg_elevation_ft,
                                        d.percent_foreign_born,
                                        d.native_american,
                                        d.asian,
                                        d.hispanic_or_latino,
                                        d.black,
                                        d.white
                                        FROM immigration m JOIN temperature t ON m.state_code=t.state_abbreviations AND m.month=t.month
                                        JOIN demographics d ON d.state_code=t.state_abbreviations
                                        JOIN airport a ON a.state=t.state_abbreviations
                                        GROUP BY m.year,m.month, m.origin_country,\
                                        m.dest_state_name,m.state_code,t.avg_temp_fahrenheit,a.avg_elevation_ft,\
                                        d.percent_foreign_born,d.native_american,\
                                        d.asian,d.hispanic_or_latino,\
                                        d.hispanic_or_latino,d.white,\
                                        d.black
                                        ORDER BY m.origin_country,m.state_code
    """)
    # Convert Immigration Fact Table to data frame
    #print('Converting Immigration Fact Table to data frame...')
    #immigration_to_states.toDF('year', 'immigration_month', 'immigration_origin', 'to_immigration_state', \
    #          'to_immigration_state_count', 'avg_temp_fahrenheit', 'avg_elevation_ft',\
    #          'pct_foreign_born', 'native_american', 'asian', 'hispanic_or_latino', 'black', 'white').show(5)
    #print('Converted Immigration Fact Table to data frame!')
    
    # Write to parquet file
    print('Writing Immigration Fact Table to Parquet...')
    immigration_to_states.write.mode('overwrite').parquet("immigration_to_states")

    print('Exiting build_fct_tbl')
    return immigration_to_states

def data_quality_checks(immigration_to_states):
    """ Purpose of this function is to peroform DQ validations
    """        
    # Cnt tot num of rows in FACT Table.
    tot_num_rows_cnt= immigration_to_states.select(sum('to_immigration_state_count').alias('fact_table_count'))

    # check for NULL values #must have 'false' for it to be vaild
    null_value_rows_cnt = immigration_to_states.select(isnull('year').alias('year'),\
                                 isnull('immigration_month').alias('month'),\
                                 isnull('immigration_origin').alias('country'),\
                                 isnull('to_immigration_state').alias('state')).dropDuplicates()
    
    print('Exiting data_quality_checks')
    return tot_num_rows_cnt, null_value_rows_cnt

def main():
    """ MAIN program - issues necessary calls in required sequence
        for processing
    """
    print('Create spark session and sqlContext')
    spark, sqlContext =  create_sparksess_sqlctx()
    
    print('Read all lookup data to store the codes in respective dictionary objects')
    valid_port_i94, country_codes, state_codes, state_codes_rev, city_codes = read_lookups()

    print('UDF functions to access corresponding dictionary objects in previous step')
    i94port_udf=udf(lambda x: valid_port_i94[x],StringType())        
    country_udf=udf(lambda x: country_codes[x],StringType())        
    state_udf=udf(lambda x: state_codes[x],StringType())        
    state_udf_rev=udf(lambda x: state_codes_rev[x],StringType())        
    city_udf=udf(lambda x: city_codes[x],StringType())        

    print('Read files into SPARK DATAFRAMES')
    i94_file_data, demo_file_data, airport_file_data, temp_file_data = read_data_files(spark)
    
    print('Demography data processing')
    final_demographics_data = demograph_proc(demo_file_data)

    print('Temperature data processing')
    final_temperature_data = temperature_proc(temp_file_data, state_codes_rev, state_udf_rev)
    
    print('Airport data processing')
    final_airport_data = airports_proc(airport_file_data)
    
    print('Immigration data processing')
    final_i94_data = immidata_proc(i94_file_data, state_codes, city_codes, country_udf, state_udf, city_udf)

    print('creating dimension tables')
    sqlContext, temperature, immigration, demographics, airport = create_dim_tables(final_demographics_data, final_temperature_data , final_i94_data, final_airport_data, sqlContext)
    
    print('create fact tables')
    immigration_to_states = build_fct_tbl(temperature, immigration, demographics, airport, spark)
    
    print('performing data quality checks')
    tot_num_rows_cnt, null_value_rows_cnt = data_quality_checks(immigration_to_states)
    print('total rows count in fact table')
    tot_num_rows_cnt.show()
    print('null_valus_rows_cnt below - this should have FALSE to be considered valid or no issues')
    null_value_rows_cnt.show()

if __name__ == "__main__":
    main()