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
from haversine import haversine, Unit


# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We get the parameter list from the line
    params_list = line.strip().split(",")

    # (00) Date => The date of the measurement. String <%Y-%m-%d %H:%M:%S> (e.g., "2013-01-01 13:00:02").
    # (01) Bus_Line => The bus line. Int (e.g., 120).
    # (02) Bus_Line_Pattern => The pattern of bus stops followed by the bus. String (e.g., "027B1001"). It can be empty (e.g., "").
    # (03) Congestion => On whether the bus is at a traffic jam (No -> 0 and Yes -> 1). Int (e.g., 0).
    # (04) Longitude => Longitude position of the bus. Float (e.g., -6.269634).
    # (05) Latitude = > Latitude position of the bus. Float (e.g., 53.360504).
    # (06) Delay => Delay of the bus in seconds (negative if ahead of schedule). Int (e.g., 90).
    # (07) Vehicle => An identifier for the bus vehicle. Int (e.g., 33304)
    # (08) Closer_Stop => An idenfifier for the closest bus stop given the current bus position. Int (e.g., 7486). It can be no bus stop, in which case it takes value -1 (e.g., -1).
    # (09) At_Stop => On whether the bus is currently at the bus stop (No -> 0 and Yes -> 1). Int (e.g., 0).

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
            bucket_size,
            max_speed_accepted,
            day_picked
            ):
    # 1. Operation C1: 'textFile'
    inputRDD = sc.textFile(my_dataset_dir)
    date_format = "%Y-%m-%d %H:%M:%S"
    # ---------------------------------------
    # TO BE COMPLETED
    # ---------------------------------------

    f1RDD = inputRDD.filter(lambda x: day_picked in x)
    f2RDD = f1RDD.map(process_line)\
                 .sortBy(lambda x: datetime.datetime.strptime(x[0], date_format))\
                 .sortBy(lambda x:x[7])


    f3RDD = f2RDD.map(lambda x: (x[7], (datetime.datetime.strptime(x[0], date_format), x[5], x[4], 0)))

    def get_distance_function(point1, point2):

        p1 = (point1[1], point1[2])
        p2 = (point2[1], point2[2])

        dist = haversine(p1, p2, unit=Unit.METERS)
        time_gap = abs(point2[0] - point1[0]).total_seconds()

        speed = dist / time_gap

        if (speed > max_speed_accepted):
            dist = 0

        final_time = point2[0]
        point = (point2[1], point2[2])
        if point2[0] < point1[0]:
            final_time = point1[0]
            point = (point1[1], point1[2])

        return (final_time, point[0], point[1], dist + point1[3] + point2[3])



    f4RDD = f3RDD.reduceByKey(get_distance_function) \
                 .map(lambda x: (x[0], x[1][3] / 1000))

    def make_fill_bucket(param):
        bucket_index = int(param[1] / bucket_size)
        range_interval = f"{int(bucket_index * bucket_size)}_{int(bucket_index * bucket_size) + bucket_size}"

        return (range_interval,(bucket_index, 1))

    f5RDD = f4RDD.map(make_fill_bucket)\
                  .reduceByKey(lambda x,y: (x[0],x[1]+y[1]))\


    prenSolRDD = f5RDD.map(lambda row: (row[1][0],(row[0],row[1][1])))\
                       .sortByKey()

    solutionRDD = prenSolRDD.map(lambda row:(row[0],row[1][0],row[1][1]))

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
    bucket_size = 50
    max_speed_accepted = 28.0
    day_picked = "2013-01-07"

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    # my_local_path = "../../../../3_Code_Examples/L07-23_Spark_Environment/"
    # my_databricks_path = "/"

    my_dataset_dir = "A01_Datasets/my_dataset_complete/"
    #my_dataset_dir = "A01_Datasets/A01_ex4_micro_dataset_1/"
    #my_dataset_dir = "A01_Datasets/A01_ex4_micro_dataset_2/"
    #my_dataset_dir = "A01_Datasets/A01_ex4_micro_dataset_3/"

    # if local_False_databricks_True == False:
    #    my_dataset_dir = my_local_path + my_dataset_dir
    # else:
    #    my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    start_time = time.time()

    my_main(sc,
            my_dataset_dir,
            bucket_size,
            max_speed_accepted,
            day_picked
            )

    total_time = time.time() - start_time
    print("Total time = " + str(total_time))
