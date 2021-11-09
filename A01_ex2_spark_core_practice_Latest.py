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



# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc,
            my_dataset_dir,
            vehicle_id,
            day_picked,
            delay_limit
            ):
    # 1. Operation C1: 'textFile'
    inputRDD = sc.textFile(my_dataset_dir)

    # ---------------------------------------
    # TO BE COMPLETED
    # ---------------------------------------
    splitlineRDD = inputRDD.map(process_line)

    # Filter the row based on filtering condition
    # Matching date, vehicle id and should stop at the station
    filteredRDD = splitlineRDD.filter(lambda row: \
                                          row[7] == vehicle_id and \
                                          row[9] != 0 and \
                                          row[0][:10] == day_picked)

    # keep busline, bus stop,date,Delay column
    # sort by busline and date
    timesortedRDD = filteredRDD.map(
        lambda row: (row[1], row[8], datetime.datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"), row[6])) \
        .sortBy(lambda row: (row[0],row[2]))
        
    #generate row number for records 
    # make row number as part of tuple
    #The resultant RDD will have bus lineid,bus stop id as key
    # Time,delay & row number as value
    genRowNumRDD = timesortedRDD.zipWithIndex()\
                                .map(lambda row: ((row[0][0], row[0][1]), (row[0][2], row[0][3], row[1])))

    #group the records based on key and collect all the value in list
    # so resultant RDD will have (busline id,station id) as key and
    # value as list of tuple value
    # ((lineid,stationid),[(time,delay,rownum),(time,delay,rownum),(time,delay,rownum)..])
    grpbyKeyRDD =  genRowNumRDD.groupByKey()\
                               .mapValues(list)    



    def filterValuesBasedonRowNum(row):
        data_to_keep = []
        index = -1
        for item in row:
            if (index < 0):
                data_to_keep.append(item)
            else:
                if item[2] != index + 1:
                    data_to_keep.append(item)
            index = item[2]

        return data_to_keep

    # As the vales are list of tuple (time,delay,rownum) and need to redue the list
    # to keep only those tuple in list for which row number are not in sequential order
    # Tuple in list will have row number in increasing order
    mapValueCustFunRDD = grpbyKeyRDD.mapValues(filterValuesBasedonRowNum) 


    def flatMapFun(row):
        key = row[0]
        value = row[1]
        ret_val = []
        for item in value:
            ret_val.append((key, item))
        return ret_val

    # After reduction of list value in previous step, the list
    # needs to flatten to bring it into original RDD form
    # The Value list may have more than one item
    #The flatten row will share the key
    # (key,[value1,value2])-> (key,value1),(key,value2)
    flatmapCustFunRDD = mapValueCustFunRDD.flatMap(flatMapFun) 

    #Map the RDD back toyuple of line id,busstop id, time, delay and sort by time
    normaliserowRDD = flatmapCustFunRDD.map(lambda row: (row[0][0], row[0][1], row[1][0], row[1][1])) \
                                       .sortBy(lambda row: row[2])

    # based on delay limit ,set the delay flag as 0 or 1
    solutionRDD = normaliserowRDD.map(
        lambda row: (row[0], row[1], row[2].strftime('%H:%M:%S'), 1 if abs(row[3]) <= abs(delay_limit) else 0))

    
    # Operation A1: 'collect'
    # print(solutionRDD)
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
    vehicle_id = 33145
    day_picked = "2013-01-02"
    delay_limit = 60

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../../3_Code_Examples/L07-23_Spark_Environment/"
    my_databricks_path = "/"

    #my_dataset_dir = "C:/gyani/Msc/BigData/A01_Datasets/my_dataset_complete/"
    my_dataset_dir = "C:/gyani/Msc/BigData/A01_Datasets/A01_ex2_micro_dataset_1/"
    #my_dataset_dir = "C:/gyani/Msc/BigData/A01_Datasets/A01_ex2_micro_dataset_2/"
    #my_dataset_dir = "FileStore/tables/6_Assignments/A01_ex2_micro_dataset_3/"
    
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

    my_main(sc, my_dataset_dir, vehicle_id, day_picked, delay_limit)

    total_time = time.time() - start_time
    print("Total time = " + str(total_time))
