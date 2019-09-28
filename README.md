Step 1: Scope the Project and Gather Data
	Scope : To combine 4 different data sources and derive a FACT table 
		    that will hold the immigration data movement in USA
	Data :
			UDACITY provided data - World temperature, city demography, airport codes, i94 immigration data
			
Step 2: Explore and Assess the Data
	Data cleansing:
		Demographics-data : 
			create a DF with %'s and pivoted race attribute
		Temperature-data :
			create a DF to hold just the USA information for year 2013
		Airports-data:
			Perform necessary cleansing to hold valid needed data in the end DF
		Immigration-data:
			Filter for state/city in the udf list, and perform necessary datatype conversion

Step 3: Define the Data Model
		Dimension Tables
			demographics:	Attributes - median_age, percent_male_population, percent_female_population, percent_veterans, 
										percent_foreign_born, native_american, asian, hispanic_or_latino, black, white, 
										State, state_code
			immigration:	Attributes - origin_country, i94port, city_port_us, state, destination_state_us, cicid, 
										year, month,
			airport: 		Attributes - country, state, avg_elevation_ft
			temperature:	Attributes - average_temperature, avg_temp_fahrenheit, state_abbreviations, State,Country, 
										year, month
		
		Fact Table
			immigration_fact_table:	Attributes - immigration_origin, immigration_state, to_immigration_state_count,
												avg_temp_fahrenheit, avg_elevation_ft, percent_foreign_born, 
												native_american, asian, hispanic_or_latino, black, white, year, 
												immigration_month
		
		Fact table derived to a spark dataframe & written as parquet file.
		
Step 4: Run ETL to Model the Data
		Below are the sequence of steps to accomplish the scope
			1)	Spark and sqlContext are created
			2)	All lookup data to reference are stored in a dictionary object (city,state,country,port codes)
			3)  UDF functions created for those dictionary
			4)	Read files into the SPARK DATAFRAME
			5)	demography data processing
			6)	temperature data processing
			7) 	airport data processing
			8) 	immigration data processing
			9)	create dimension tables from 5, 6, 7, 8 steps above.
			10)	create fact table & write it to a parquet file
			11) perform DQ validations 
			
Step 5: Complete Project Write Up
	What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?
		This project uses pyspark to accomplish the goal. This can be hosted in Amazon EMR with AIRFLOW installed 
		with the source data residing in S3 with target parquet in another S3 bucket, and scheduled with AIRFLOW 
	
Document the steps of the process.
	Propose how often the data should be updated and why.
		If we are using Airflow this can be set to run on desired frequency. We will be able to add incremental data 
		into the existing file, and process it as a complete data everytime. Or we can segregate it to have separate yearly filesa
		and derive the target fact table data for specific year.
		
Include a description of how you would approach the problem differently under the following scenarios:
	If the data was increased by 100x.
		This code can be put in EMR which will be able to handle huge data sizes with decent performance.
	The pipelines would be run on a daily basis by 7 am every day.
		Apache Airflow will help schedule to run this on set scheduled time.  If we change to incremental updated
		mode, even if SLA not met due to some failure, it will still have aggregated data till previous day's execution.
	The database needed to be accessed by 100+ people.
		=>If number of users accessing data goes multifold, one option is to involve NoSQL databases like 
		Apache Cassandra or Dynamodb.  If using AWS stack, dynamodb would be natural choice.
		=>Another option to consider would be put the parquet files in HDFS file system in all nodes in EMR 
		and grant access to the users.
Choice of tools, technologies, and data model:
	Spark, Python, Parquet files.
	As the immigration data was huge, and multiple file formats (including SAS) involved, SPARK became a natural choice of tool. 
	Spark SQL was used to handle large files converted into dataframes & results are derived through regular SQL join operations 
	to accomplish the final FACT table.    