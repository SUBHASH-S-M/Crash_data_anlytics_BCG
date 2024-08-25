class loaddata:
    def load_dataframe(self,spark,job_setup_object,key):
        path_to_read=job_setup_object.base_path+job_setup_object.sourcedatabase_path+'/'+job_setup_object.source_mapping[key]
        job_setup_object.global_logger.info(f"reading the path {path_to_read}")
        return spark.read.csv(path_to_read,header=True,inferSchema=True)
class write_date:
    def write_to_file(self,dataframe_to_write,path,format,base_obj):
        base_obj.glogger.info("Writing result sets")

        if(format.strip()=='csv'):
            dataframe_to_write.coalesce(1) \
                .write.option("header", True) \
                .option("delimiter", ",") \
                .format("csv") \
                .mode("overwrite") \
                .save(f"{path}")
        elif(format.strip=='parquet'):
            dataframe_to_write.write.parquet(f"{path}.parquet")
class analyser_parser:
    def parse_question(self,spark,job_setup_object ):
        mapper_dict=job_setup_object.driver_obj.analysis_mapper
        write_path=job_setup_object.driver_obj.base_path+job_setup_object.driver_obj.destination_path
        write_file_format=job_setup_object.driver_obj.destination_format
        self.glogger=job_setup_object.driver_obj.global_logger
        
        for analysis_set in mapper_dict.keys():
            print(analysis_set)
            self.glogger.info(f"________________analysing {analysis_set}______________________")
            self.glogger.info(analysis_set)
            if(analysis_set=='all_anlysis' ):
                continue
            if ( analysis_set =='get_crashes_with_high_deaths'):
                df=job_setup_object.get_crashes_with_high_deaths(mapper_dict[analysis_set].split('|')[0])
                writepathloc=write_path+f"/{analysis_set}"
                write_date().write_to_file(df,writepathloc,write_file_format,self)
            elif ( analysis_set =='count_two_wheelers'):
                df=job_setup_object.count_two_wheelers()
                writepathloc=write_path+f"/{analysis_set}"
                write_date().write_to_file(df,writepathloc,write_file_format,self)
            elif ( analysis_set =='get_top_vehicle_makes'):
                df=job_setup_object.get_top_vehicle_makes(mapper_dict[analysis_set].split('|')[0])
                writepathloc=write_path+f"/{analysis_set}"
                write_date().write_to_file(df,writepathloc,write_file_format,self)
            elif ( analysis_set =='count_hit_and_run_vehicles'):
                df=job_setup_object.count_hit_and_run_vehicles()
                writepathloc=write_path+f"/{analysis_set}"
                write_date().write_to_file(df,writepathloc,write_file_format,self)
            elif ( analysis_set =='get_top_states_with_highest_accidents'):
                df=job_setup_object.get_top_states_with_highest_accidents(mapper_dict[analysis_set].split('|')[0])
                writepathloc=write_path+f"/{analysis_set}"
                write_date().write_to_file(df,writepathloc,write_file_format,self)
            elif ( analysis_set =='get_top_vehicle_injuries'):
                df=job_setup_object.get_top_vehicle_injuries(mapper_dict[analysis_set].split('|')[0],mapper_dict[analysis_set].split('|')[1])
                writepathloc=write_path+f"/{analysis_set}"
                write_date().write_to_file(df,writepathloc,write_file_format,self)
            elif ( analysis_set =='get_top_ethnic_groups_per_body_style'):
                df=job_setup_object.get_top_ethnic_groups_per_body_style()
                writepathloc=write_path+f"/{analysis_set}"
                write_date().write_to_file(df,writepathloc,write_file_format,self)
            elif ( analysis_set =='get_top_zipcodes_with_alcohol_crashes'):
                df=job_setup_object.get_top_zipcodes_with_alcohol_crashes()
                writepathloc=write_path+f"/{analysis_set}"
                write_date().write_to_file(df,writepathloc,write_file_format,self)
            elif ( analysis_set =='analyze_crash_data'):
                df=job_setup_object.analyze_crash_data()
                writepathloc=write_path+f"/{analysis_set}"
                write_date().write_to_file(df,writepathloc,write_file_format,self)
            elif ( analysis_set =='get_top_5_vehicle_makes'):
                df=job_setup_object.get_top_5_vehicle_makes()
                writepathloc=write_path+f"/{analysis_set}"
                write_date().write_to_file(df,writepathloc,write_file_format,self)
            self.glogger.info("________________****************_______________________")
        return True


