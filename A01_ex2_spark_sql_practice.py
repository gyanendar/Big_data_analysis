# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

import pyspark
import time
import pyspark.sql.functions as f
from pyspark.sql.window import Window

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark,
            my_dataset_dir,
            vehicle_id,
            day_picked,
            delay_limit
           ):

    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("busLineID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("busLinePatternID", pyspark.sql.types.StringType(), False),
         pyspark.sql.types.StructField("congestion", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("longitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("latitude", pyspark.sql.types.FloatType(), False),
         pyspark.sql.types.StructField("delay", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("vehicleID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("closerStopID", pyspark.sql.types.IntegerType(), False),
         pyspark.sql.types.StructField("atStop", pyspark.sql.types.IntegerType(), False)
         ])

    # 2. Operation C2: 'read'
    inputDF = spark.read.format("csv") \
        .option("delimiter", ",") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # ---------------------------------------
    # TO BE COMPLETED
    # ---------------------------------------
    
    # Apply first filtering on date, vechile id, and atStop
    filteredDF = inputDF.select(f.col("*")).where((f.col("atStop") != 0) & \
                                            (f.col("vehicleID") == vehicle_id) &\
                                            (f.to_date(f.lit(day_picked),"yyyy-MM-dd") == f.to_date(inputDF["date"],"yyyy-MM-dd HH:mm:ss")) )
    # Sort the rows based on date
    timesortedDF= filteredDF.orderBy(f.to_timestamp(filteredDF["date"]))

    
    # Add new column having time of date and rename the existing column based on expected final output
    # Select the necessary columns only
    timeStampDF = timesortedDF.select(f.col("busLineID").alias("lineID"),f.col("closerStopID").alias("stationID"),f.col("delay"),f.col("date"))\
               .withColumn("arrivalTime",f.date_format(timesortedDF["date"],"HH:mm:ss"))

    #order the row based on time stamp columns arrivalTime
    datatoWorkDF = timeStampDF.select(f.col("lineID"),f.col("stationID"),f.col("arrivalTime"),f.col("delay"))\
               .orderBy(f.col("arrivalTime"))
    
    # Add delay flag column as per expected output
    delayFlagAddedDF = datatoWorkDF.withColumn("onTime",f.when(f.abs(datatoWorkDF["delay"])>=delay_limit,0)\
               .otherwise(1))
    # discard delay column
    finalWorkingDF = delayFlagAddedDF.select(f.col("lineID"),f.col("stationID"),f.col("arrivalTime"),f.col("onTime"))
    
    # Generate row number as coulmn
    w = Window().orderBy(f.lit('A'))
    dataWithRowNumberDF = finalWorkingDF.withColumn("row_num", f.row_number().over(w))

    #clone dataWithRowNumberDF with renamed column name to differentiate bewteen columns coming from different DF
    cloneDF = dataWithRowNumberDF.select(f.col("lineID").alias("lineID_1"),\
                                      f.col("stationID").alias("stationID_1"),\
                                      f.col("arrivalTime").alias("arrivalTime_1"),\
                                      f.col("onTime").alias("onTime_1"),\
                                      f.col("row_num").alias("row_num_1"))
    
    # Here need rows based on incrementing time 
    # Doing cross join to get all combination                                      
    multipliedDF = dataWithRowNumberDF.crossJoin(cloneDF)  

    # Row number generated from 1..N
    # line id& station id having no repeatition
    # Need to keep the combination where 2nd part coming as next rownumber 
    keepNextRowDF = multipliedDF.select(f.col("*")).where((multipliedDF["row_num_1"]-multipliedDF["row_num"]==1)|((multipliedDF["row_num_1"]==1)&(multipliedDF["row_num"]==1)))   
     
    # filter out the row here station id and line is matching 
    removeMatchingLineStationIDDF = keepNextRowDF.select(f.col("*")).where(~((f.col("lineID") == f.col("lineID_1"))& (f.col("stationID") == f.col("stationID_1")))|((f.col("row_num_1")==1)&(f.col("row_num")==1)))

    # Rename the columns as per expected output
    solutionDF = removeMatchingLineStationIDDF.select(f.col("lineID_1").alias("lineID"),f.col("stationID_1").alias("stationID"),f.col("arrivalTime_1").alias("arrivalTime"),f.col("onTime_1").alias("onTime"))
    
    # ---------------------------------------
    
    # Operation A1: 'collect'
    resVAL = solutionDF.collect()
    for item in resVAL:
        print(item)

# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Once our computer has finished processing the PYTHON PROGRAM DEFINITION section its knowledge is set.
# Now its time to apply this knowledge.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer finally processes this PYTHON PROGRAM EXECUTION section, which:
# (i) Specifies the function F to be executed.
# (ii) Define any input parameter such this function F has to be called with.
#
# --------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    vehicle_id = 33145
    day_picked = "2013-01-02"
    delay_limit = 60

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    #my_local_path = "../../../../3_Code_Examples/L07-23_Spark_Environment/"
    #my_databricks_path = "/"

    my_dataset_dir = "C:/gyani/Msc/BigData/A01_Datasets/my_dataset_complete/"
    #my_dataset_dir = "C:/gyani/Msc/BigData/A01_Datasets/A01_ex2_micro_dataset_1/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex2_micro_dataset_2/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex2_micro_dataset_3/"

    #if local_False_databricks_True == False:
    #    my_dataset_dir = my_local_path + my_dataset_dir
    #else:
    #    my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    start_time = time.time()

    my_main(spark, my_dataset_dir, vehicle_id, day_picked, delay_limit)

    total_time = time.time() - start_time
    print("Total time = " + str(total_time))

