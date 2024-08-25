#spark submit need to change the export params alone as per requirement
export BASE_PATH=/Users/ssm7/IdeaProjects/case_study_analysis
export SPARK_HOME=/Users/ssm7/Library/Python/3.12/lib/python/site-packages/
$SPARK_HOME/bin/spark-submit \
     --deploy-mode client \
     --py-files $BASE_PATH/dist/crashanalysis-0.0.1-py3-none-any.whl \
     $BASE_PATH/scripts/driver.py \
     $BASE_PATH/


