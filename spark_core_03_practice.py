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
import datetime

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We get the parameter list from the line
    params_list = line.strip().split(",")

    #(00) Date => The date of the measurement. String <%Y-%m-%d %H:%M:%S> (e.g., "2013-01-01 13:00:02").
    #(01) Bus_Line => The bus line. Int (e.g., 120).
    #(02) Bus_Line_Pattern => The pattern of bus stops followed by the bus. String (e.g., "027B1001"). It can be empty (e.g., "").
    #(03) Congestion => On whether the bus is at a traffic jam (No -> 0 and Yes -> 1). Int (e.g., 0).
    #(04) Longitude => Longitude position of the bus. Float (e.g., -6.269634).
    #(05) Latitude = > Latitude position of the bus. Float (e.g., 53.360504).
    #(06) Delay => Delay of the bus in seconds (negative if ahead of schedule). Int (e.g., 90).
    #(07) Vehicle => An identifier for the bus vehicle. Int (e.g., 33304)
    #(08) Closer_Stop => An idenfifier for the closest bus stop given the current bus position. Int (e.g., 7486). It can be no bus stop, in which case it takes value -1 (e.g., -1).
    #(09) At_Stop => On whether the bus is currently at the bus stop (No -> 0 and Yes -> 1). Int (e.g., 0).

    # 3. If the list contains the right amount of parameters
    if (len(params_list) == 10):
        # 3.1. We set the right type for the parameters
        params_list[1] = int(params_list[1])
        params_list[3] = int(params_list[3])
        params_list[4] = float(params_list[4])
        params_list[5] = float(params_list[5])
        params_list[6] = int(params_list[6])
        params_list[7] = int(params_list[7])
        params_list[8] = int(params_list[8])
        params_list[9] = int(params_list[9])

        # 3.2. We assign res
        res = tuple(params_list)

    # 4. We return res
    return res

def filter_function(para,current_time,seconds_horizon):
    mylist = para.split(",")

    if(mylist[9]=='0'):
        return False

    d1 = datetime.datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S") 
    d = datetime.datetime.strptime(mylist[0], "%Y-%m-%d %H:%M:%S") 
    d2 = d1+datetime.timedelta(seconds=seconds_horizon)
    
    if(not(d1<=d<=d2)):
        return False

    return True

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc,
            my_dataset_dir,
            current_time,
            seconds_horizon
           ):

    # 1. Operation C1: 'textFile'
    inputRDD = sc.textFile(my_dataset_dir)

    

    # ---------------------------------------
    # TO BE COMPLETED
    # ---------------------------------------
    star_time = datetime.datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S")
    cutoff_time = star_time + datetime.timedelta(seconds=seconds_horizon)

    rowRDD = inputRDD.map(process_line)
    
    busStoppedRDD = rowRDD.filter(lambda row:row[9]==1)\
                          .map(lambda row:(datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"),row[8],row[7]))                           

    finalfilteredRDD = busStoppedRDD.filter(lambda row:star_time<=row[0]<=cutoff_time) 

    busstopVechileRDD = finalfilteredRDD.map(lambda row:(row[1],row[2]))

    busstopgroupedRDD = busstopVechileRDD.groupByKey()\
                                         .map(lambda row:(row[0],sorted(list(set(row[1])))))


    busstopgroupedRDD.persist()

    lenRDD = busstopgroupedRDD.map(lambda row:len(row[1]))
                             
    max_len = lenRDD.max()

    solutionRDD = busstopgroupedRDD.filter(lambda row: len(row[1]) == max_len)

   
     
    # ---------------------------------------
    
    # Operation A1: 'collect'
    print(solutionRDD)
    resVAL = solutionRDD.collect()
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
    current_time = "2013-01-07 06:30:00"
    seconds_horizon = 1800

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../3_Code_Examples/L07-23_Spark_Environment/"
    my_databricks_path = "/"

    my_dataset_dir = "A01_Datasets/my_dataset_complete/"
    #my_dataset_dir = "A01_Datasets/A01_ex3_micro_dataset_1/"
    #my_dataset_dir = "C:/gyani/Msc/BigData/A01_Datasets/A01_ex3_micro_dataset_2/"
    #my_dataset_dir = "C:/gyani/Msc/BigData/A01_Datasets/A01_ex3_micro_dataset_3/"

    #if local_False_databricks_True == False:
    #    my_dataset_dir = my_local_path + my_dataset_dir
    #else:
    #    my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    start_time = time.time()

    my_main(sc, my_dataset_dir, current_time, seconds_horizon)

    total_time = time.time() - start_time
    print("Total time = " + str(total_time))
