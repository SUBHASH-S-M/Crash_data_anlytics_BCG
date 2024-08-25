
import json
class jobSetup():
    def __init__(self,global_logger,base_path,job_config_path):
        """
        Initializes a jobSetup object.

        Args:
            job_config_path (str): The path to the job configuration file.

        """
        self.global_logger=global_logger
        self.base_path=base_path
        data_driver=self.load_json(base_path+job_config_path)
        self.global_logger.info(f"driver json parsing successfull {job_config_path}")

        self.sourcedatabase_path=data_driver['source_details']['source_path']
        self.source_mapping=data_driver['source_details']['source_mapping']

        self.destination_path=data_driver['destination_details']['destinatnation_path']
        self.destination_format=data_driver['destination_details']['destination_fileformat']

        analyse_json_parser=self.load_json(base_path+data_driver['analysis_details']['analysis_path'])
        self.global_logger.info(f"analysis json parsing successfull {base_path+data_driver['analysis_details']['analysis_path']}")
        self.analysis_mapper=analyse_json_parser['analysis_number_parmas']
        self.global_logger.info("job setup object initialised successfully")


    def load_json(self,job_config_path):

        """
        Loads the job configuration file into a dictionary.

        Args:
            job_config_path (str): The path to the job configuration file.
        """
        self.global_logger.info(f"json parsing {job_config_path}")
        with open(job_config_path , "r") as file:
            data = json.load(file)
        return data

