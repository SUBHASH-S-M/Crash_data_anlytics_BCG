




from crashanalysis.jobSetup import jobSetup
from crashanalysis.loggerlib import JobLoggers
from crashanalysis.anlaysis import analyse_snippets
from pyspark.sql import SparkSession

import datetime
import os
import sys



if __name__ == '__main__':


    current_directory = os.getcwd()

    #if env is dev then it take local directory
    if("Users/ssm7" in current_directory):
        sys.argv=[0,0]
        sys.argv[1]="/Users/ssm7/IdeaProjects/Crash_data_anlytics_BCG/"


    base_path=sys.argv[1]
    # base_path="/Users/ssm7/IdeaProjects/case_studies_anlysis/"
    #setting up log directory
    spark = SparkSession.builder.appName("SuppressWarning").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    log_file_name="data_anlysis_"+str(datetime.datetime.now())
    log_dir=f"""{base_path}logs"""

    logger_obj = JobLoggers(log_file_name,"debug","%(asctime)s - %(levelname)s - %(message)s","%m/%d/%Y %I:%M:%S %p",True,log_dir)
    global_logger= logger_obj.logger
    global_logger.info(f'loggers initialised succesfully at location {log_dir}')

    #initialising the job setup
    job_setup_object=jobSetup(global_logger,base_path,"/configs/driver.json")
    global_logger.info("job setup done , parsed the driver json and analysis json")
    analyser_obj=analyse_snippets(spark,job_setup_object)
    if(analyser_obj.execution_status==True):
        global_logger.info("Execute the operations successfully")


