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
from haversine import haversine, Unit
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark,
            my_dataset_dir,
            bucket_size,
            max_speed_accepted,
            day_picked
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

    # 2. Operation C1: 'read'
    inputDF = spark.read.format("csv") \
        .option("delimiter", ",") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    
    # ---------------------------------------
    # TO BE COMPLETED
    # ---------------------------------------
    rawDF = inputDF.withColumn("date",f.to_timestamp(inputDF["date"]))
    dateFilteredADF = rawDF.select(f.col("date"),f.col("vehicleID"),f.col("latitude"),f.col("longitude"))\
                            .where(f.to_date(rawDF["date"]) == f.to_date(f.lit(day_picked)))\
                            .orderBy(rawDF["date"],rawDF["vehicleID"])
        
    dateFilteredBDF = dateFilteredADF.select(f.col("date").alias("date1"),f.col("vehicleID").alias("vehicleID1"),f.col("latitude").alias("latitude1"),f.col("longitude").alias("longitude1"))


    
    vechGroupDF = dateFilteredADF.join(dateFilteredBDF, [dateFilteredADF["vehicleID"] == dateFilteredBDF["vehicleID1"],
                                                         dateFilteredADF["date"] <= dateFilteredBDF["date1"],
                                                         dateFilteredADF["date"] != dateFilteredBDF["date1"]                                                         
                                                            ],"inner")

    filtermatchingDF = vechGroupDF.groupBy(["vehicleID","date"]).agg({"date1":"min"})

    timingDF = filtermatchingDF.select(f.col("vehicleID").alias("vehicleID_1"),f.col("date").alias("date_1"),f.col("min(date1)").alias("date_2"))

    finaljoinedDF = vechGroupDF.join(timingDF,[vechGroupDF["vehicleID"]==timingDF["vehicleID_1"],
                               vechGroupDF["date"]==timingDF["date_1"],
                               vechGroupDF["date1"]==timingDF["date_2"]],"inner")
    finalDF = finaljoinedDF.select(f.col("vehicleID"),f.col("date").alias("start"),f.col("date_2").alias("end"),f.col("latitude").alias("lat1"),f.col("longitude").alias("lon1"),f.col("latitude1").alias("lat2"),f.col("longitude1").alias("lon2"))

    def get_distance_function(lat1,long1,lat2,long2,t1,t2):
       
        p1 = (lat1,long1)
        p2 = (lat2,long2)
        
        dist =  haversine(p1, p2,unit=Unit.METERS)
        time_gap = abs(t2-t1).total_seconds()

        speed = dist/time_gap
        
        if(speed>max_speed_accepted):
            print("Speed Exceeded:",dist)
            dist = 0

        return dist/1000
    
    distance_udf = f.udf(get_distance_function,pyspark.sql.types.FloatType())
    finalWithDstDF = finalDF.withColumn("distance",distance_udf("lat1","lon1","lat2","lon2","start","end"))
    finalDstDF =  finalWithDstDF.select(f.col("vehicleID"),f.col("distance"))                            
    finalvecdisDF = finalDstDF.groupBy("vehicleID").agg({"distance":"sum"}).withColumnRenamed("sum(distance)", "totaldistance")
    bucket1DF = finalvecdisDF.select(f.col("vehicleID"),(f.col("totaldistance")/f.lit(bucket_size)).cast(pyspark.sql.types.IntegerType()).alias("bucket_id"))
    bucket2DF = bucket1DF.select(f.col("vehicleID"),f.col("bucket_id"),(f.col("bucket_id")*f.lit(bucket_size)).cast(pyspark.sql.types.StringType()).alias("start"),(f.col("bucket_id")*f.lit(bucket_size)+f.lit(bucket_size)).cast(pyspark.sql.types.StringType()).alias("end"))
    bucket3DF = bucket2DF.select(f.col("vehicleID"),f.col("bucket_id"),f.col("start"), f.concat("start",f.lit("_"),"end").alias("bucket_size"))
    bucket4DF = bucket3DF.groupBy("vehicleID").agg({"bucket_id":"count"}).withColumnRenamed("count(bucket_id)", "num_vehicles")
    bucket5DF = bucket4DF.join(bucket3DF,bucket3DF["vehicleID"]==bucket4DF["vehicleID"],"inner")
    solutionDF = bucket5DF.select(f.col("bucket_id"),f.col("bucket_size"),f.col("num_vehicles")).orderBy(bucket5DF["bucket_id"].asc())
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
    bucket_size = 1
    max_speed_accepted = 28.0
    day_picked = "2013-01-07"

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../3_Code_Examples/L07-23_Spark_Environment/"
    my_databricks_path = "/"

    #my_dataset_dir = "C:/gyani/Msc/BigData/A01_Datasets/my_dataset_complete/"
    my_dataset_dir = "C:/gyani/Msc/BigData/A01_Datasets/A01_ex4_micro_dataset_1/"
    #my_dataset_dir = "C:/gyani/Msc/BigData/A01_Datasets/A01_ex4_micro_dataset_2/"
    #my_dataset_dir = "C:/gyani/Msc/BigData/A01_Datasets/A01_ex4_micro_dataset_3/"

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

    my_main(spark,
            my_dataset_dir,
            bucket_size,
            max_speed_accepted,
            day_picked
           )

    total_time = time.time() - start_time
    print("Total time = " + str(total_time))
