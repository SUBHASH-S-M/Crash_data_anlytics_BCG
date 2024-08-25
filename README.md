

# Crash Data Analytics BCG

Welcome to the **Crash Data Analytics BCG** repository! 🚗📊 This project focuses on analyzing crash data using data analytics techniques and tools.

## 🚀 Project Overview

This repository contains code and resources for analyzing crash data:

- **Profiling**: Data is profiled to check the missing values , null checks and category checks.
- **Config Oriented `Flexibility`**:You can run the customized parameter for the analytics questions as input(any number of questions can be chosen for analysis)  .You can choose `.csv or .parquet` file as output of your analytics.
- **`Framework`**: `Whee file` is built similar to package , can you used as plug and play module by installing the wheel file.
- **`SOLID PRINCIPLES`**: Class structure and components  are properly utilised  as per SOLID governance, along with ` custom logging` , `exceptional handling`,`doc string`,`user comments` .

## 📁 Repository Structure

Here's a quick overview of the repository structure:

```
.
├── LICENSE
├── README.md
├── configs
│   ├── analysis_details.json
│   └── driver.json
├── crashanalysis
│   ├── __init__.py
│   ├── __pycache__
│   ├── anlaysis.py
│   ├── jobSetup.py
│   ├── loggerlib.py
│   ├── supportfunctions.py
│   └── temp.py
├── dataset
│   ├── Data
│   └── Data.zip
├── dist
│   └── crashanalysis-0.0.1-py3-none-any.whl
├── docs
│   ├── BCG_Case_Study_CarCrash_Updated_Questions.docx
│   ├── Data Dictionary.xlsx
│   └── ~$G_Case_Study_CarCrash_Updated_Questions.docx
├── logs
│   └── data_anlysis_2024-08-25 18:11:25.116556.log
├── output_analysis
│   ├── analyze_crash_data
│   ├── count_hit_and_run_vehicles
│   ├── count_two_wheelers
│   ├── get_crashes_with_high_deaths
│   ├── get_top_5_vehicle_makes
│   ├── get_top_ethnic_groups_per_body_style
│   ├── get_top_states_with_highest_accidents
│   ├── get_top_vehicle_injuries
│   ├── get_top_vehicle_makes
│   └── get_top_zipcodes_with_alcohol_crashes
├── scripts
│   ├── driver.py
│   └── spark_submit.sh
└── setup.py
```
- **Setup.py** builds the package
- **Scripts/driver.py** main function which utilises the wheel and call the other functions
- **configs/** contains driver and analysis json, separate json to maintain flexibility


### Interesting USP of configs

- **Need to adjust the metrics driven for every run ? below is the way **
```json
{
    "analysis_number_parmas":{   
    
            "all_anlysis":"Y",
            "get_crashes_with_high_deaths":"2", //male died per crash  is more than 2
            "count_two_wheelers":"",
            "get_top_vehicle_injuries":"3|5",//this 3,5 is filtering records based on the rank between 3 and 5
            "get_top_vehicle_makes": "5",
            "count_hit_and_run_vehicles": "",
            "get_top_states_with_highest_accidents": "1",
            "get_top_ethnic_groups_per_body_style": "",
            "get_top_zipcodes_with_alcohol_crashes": "",
            "analyze_crash_data": "",
            "get_top_5_vehicle_makes": ""
    }
}
```
## Utilisation
- **dataset** load the datasets in the folder (plse see the tree above)
- **config** adjust the config as required number of questions and their params


#####  The below step will ensure you have proper dependencies  installed
```bash
#basic lib installation
pip3 install requirements.txt
```
```bash
#framework wheel installation
pip3 install crashanalysis-0.0.1-py3-none-any.whl
```
#####  spark submit call
```bash
#BASE_PATH(codes base path) and SPARK_HOME(location where pyspark is isntalled)
export BASE_PATH=/Users/ssm7/IdeaProjects/case_study_analysis
export SPARK_HOME=/Users/ssm7/Library/Python/3.12/lib/python/site-packages/
$SPARK_HOME/bin/spark-submit \
     --deploy-mode client \
     --py-files $BASE_PATH/dist/crashanalysis-0.0.1-py3-none-any.whl \
     $BASE_PATH/scripts/driver.py \
     $BASE_PATH/
#$BASE_PATH is the base location for code reference 
```

### Dev Side

- Dev notebooks can found here and the development was done in Google Collab
  [Collab Notebook Offline](docs/dev_v2_case_study.ipynb)

