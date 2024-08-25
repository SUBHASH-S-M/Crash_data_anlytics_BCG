
from pyspark.sql.functions import col, trim, count, upper, lower, sum, regexp_extract, row_number, desc, rank
from pyspark.sql import Row,DataFrame
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from crashanalysis.supportfunctions import loaddata,analyser_parser

class analyse_snippets():
    def __init__(self,spark,job_setup_object):
        self.spark=spark
        self.driver_obj=job_setup_object
        self.load_dataobj=loaddata()
        self.execution_status=analyser_parser().parse_question(spark,self )

    def get_crashes_with_high_deaths(self,males_killed_per_crash: int)-> DataFrame:
        """
        Returns a DataFrame containing crash IDs with a count of deaths greater than males_killed_per_crash.
    
        Args:
            males_killed_per_crash (int): The minimum number of males killed per crash.
    
        Returns:
            DataFrame: A DataFrame containing crash IDs with a count of deaths greater than males_killed_per_crash.
        """
        data = [Row(no_of_crashes='not_able_to_convert')]
        try:
            if not(isinstance(males_killed_per_crash, int)):
                males_killed_per_crash=int(males_killed_per_crash)

            no_of_crashes_men_killed = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'PrimaryPerson') \
                .select('crash_id', 'PRSN_GNDR_ID', 'DEATH_CNT') \
                .filter((trim(col('PRSN_GNDR_ID')) == 'MALE') & (col('DEATH_CNT') >= 1)) \
                .groupBy('crash_id') \
                .agg(sum(col('DEATH_CNT')).alias('count_of_deaths')) \
                .filter(col('count_of_deaths') > males_killed_per_crash) \
                .select('crash_id') \
                .distinct().count()

            data = [Row(no_of_crashes=no_of_crashes_men_killed)]

        except (ValueError) as e:
            print(f"Type error occurred , not able to convert the input to int type check your input : {str(e)}")
        except Exception as e:
            print(f"An error occurred: {str(e)}")
        finally:
            return self.spark.createDataFrame(data)

    def count_two_wheelers(self)-> DataFrame:
        """
        Counts the number of two wheelers booked for crashes.

        Returns:
            int: The count of two wheelers.
        """
        data = [Row(two_wheelers_count='not_able_to_convert')]
        try:
            df = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'Unit')
            count_two_wheelers = df.filter(upper(col('VEH_BODY_STYL_ID')).rlike('MOTORCYCLE')).count()
            data = [Row(two_wheelers_count=count_two_wheelers)]
        except Exception as e:
            print(f"An error occurred: {str(e)}")

        finally:
            return self.spark.createDataFrame(data)


    def get_top_vehicle_makes(self,n_filter)-> DataFrame:

        """
        Get the top n_filter vehicle makes of the cars present in the crashes in which driver died and Airbags did not deploy.

        Args:
            n_filter (int): Number of top vehicle makes to retrieve.

        Returns:
            DataFrame: DataFrame containing the top n_filter vehicle makes.
        """
        data = [Row(top_n_makes='some issue with the parmeter')]
        top_n_vechile_makes_death_abg_notdeployed_df=self.spark.createDataFrame(data)
        try:
            # Filtering person as a driver died in the accident because airbag didn't deploy
            df_airbag_notdeployed = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'PrimaryPerson') \
                .filter((trim(col('PRSN_TYPE_ID')) == 'DRIVER') & (col('DEATH_CNT') >= 1) & (trim(col('PRSN_AIRBAG_ID')).rlike('NOT\\s*DEPLOYED'))) \
                .select(['crash_id', 'UNIT_NBR', 'PRSN_AIRBAG_ID', 'DEATH_CNT'])

            # Filtering vehicles only cars and their make details
            df_vechiledesignfilter = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'Unit') \
                .filter(trim(col('VEH_BODY_STYL_ID')).rlike('PASSENGER\\s*CAR')) \
                .select(['crash_id', 'UNIT_NBR', 'VEH_BODY_STYL_ID', 'VEH_MAKE_ID'])

            # Joining airbag deployment details with the vehicle details and finding top n_filter makes causing more deaths
            top_n_vechile_makes_death_abg_notdeployed_df = df_airbag_notdeployed.join(df_vechiledesignfilter, ['crash_id', 'UNIT_NBR'], 'inner') \
                .groupBy('VEH_MAKE_ID') \
                .agg(count(col('*')).alias('occurence_of_the_make')) \
                .withColumn('make_rank', row_number().over(Window.orderBy(desc('occurence_of_the_make')))) \
                .filter(col('make_rank') <= n_filter) \
                .select('VEH_MAKE_ID').alias('top_n_vechile_makes')


        except Exception as e:
            print(f"An error occurred: {str(e)}")
        finally:
            return top_n_vechile_makes_death_abg_notdeployed_df


    def count_hit_and_run_vehicles(self)-> DataFrame:
        """
        This function determines the number of vehicles with drivers having valid licenses involved in hit and run.

        Returns:
            int: The number of hit and run vehicles with valid drivers' licenses.
        """
        try:
            data = [Row(count_hit_and_run_vehicles='not_able_to_convert')]
            # Filtering all the negative patterns which need to be filtered negative boolean
            UNLICENSED_pattern = ['UNLICENSED', 'UNKNOWN', 'OTHER', 'NA', 'NULL']

            # Filtering records with driver having valid license
            vaid_driverlicense_crashes = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'PrimaryPerson') \
                .filter(upper(col('PRSN_TYPE_ID')) == 'DRIVER') \
                .select('DRVR_LIC_TYPE_ID', 'CRASH_ID', 'UNIT_NBR') \
                .filter(~col('DRVR_LIC_TYPE_ID').isin(UNLICENSED_pattern))

            # Pulling the records of the hit and run vehicle details
            intermed_vechile_lisc_details = vaid_driverlicense_crashes.join(
                self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'Unit').select(['CRASH_ID', 'UNIT_NBR', 'VEH_HNR_FL', 'VIN'])
                .filter(col('VEH_HNR_FL') == 'Y'),
                ['CRASH_ID', 'UNIT_NBR'], 'inner')

            # Return the count of hit and run vehicles with valid drivers' licenses
            count_hit_and_run_vehicles_count= intermed_vechile_lisc_details.count()
            data = [Row(count_hit_and_run_vehicles=count_hit_and_run_vehicles_count)]

        except Exception as e:
            # Handle any exceptions that occur during the execution of the function
            print(f"An error occurred: {str(e)}")
        finally:
            return self.spark.createDataFrame(data)

    def get_top_states_with_highest_accidents(self,top_N_states)-> DataFrame:
        """
        Returns a dataframe with the top N states that have the highest number of accidents
        in which females are not involved.

        Args:
            top_N_states (int): The number of top states to return.

        Returns:
            DataFrame: A dataframe with the top N states.
        """
        try:
            null_pattern = ['UNLICENSED', 'UNKNOWN', 'OTHER', 'NA', 'NULL']

            df= self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'PrimaryPerson') \
                .filter(~upper(col('DRVR_LIC_STATE_ID')).isin(null_pattern)) \
                .filter((upper(col('PRSN_GNDR_ID')) != 'FEMALE') | (upper(col('PRSN_GNDR_ID')) != 'NA')) \
                .groupBy('DRVR_LIC_STATE_ID') \
                .agg(count(col('DRVR_LIC_STATE_ID')).alias('count_state_wise')) \
                .withColumn('state_rank', rank().over(Window.orderBy(desc('count_state_wise')))) \
                .filter(col('state_rank') <= top_N_states) \
                .withColumn('top_state_in_acc_otherthanfemale', trim(col('DRVR_LIC_STATE_ID'))) \
                .select('top_state_in_acc_otherthanfemale')
        except Exception as e:
            print(f"An key error occurred check the schema of dataframe: {str(e)}")
            df=self.spark.createDataFrame([(str(e),)], ["issues_presists"])
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            df=self.spark.createDataFrame([(None,)], ["issues_presists"])
        finally:
            return df


    def get_top_vehicle_injuries(self,limit_start: int, limit_end: int) -> DataFrame:
        """
        Retrieves the top vehicle makes that contribute to the largest number of injuries including death.

        Args:
            limit_start (int): The starting ordinality to consider.
            limit_end (int): The ending ordinality to consider.

        Returns:
            DataFrame: A DataFrame containing the top vehicle makes.

        Raises:
            ValueError: If the limits are not valid or if the DataFrame fails to load.
            Exception: If any other error occurs during the execution.
        """
        try:
            if not isinstance(limit_start, int) or not isinstance(limit_end, int):
                limit_start=int(limit_start)
                limit_end=int(limit_end)


            df_vehicle_wise_injury_death_count = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'Unit').select(['TOT_INJRY_CNT', 'DEATH_CNT', 'VEH_MAKE_ID']) \
                .withColumn('total_injury_death_count', trim(col('TOT_INJRY_CNT')) + trim(col('DEATH_CNT'))) \
                .groupBy('VEH_MAKE_ID') \
                .agg(sum(col('total_injury_death_count')).alias('vech_wise_total_injury_death_count'))

            df_vehicle_wise_injury_death_count = df_vehicle_wise_injury_death_count \
                .withColumn("ordinality_injury_death_count", rank().over(Window.orderBy(desc('vech_wise_total_injury_death_count')))) \
                .filter(col('ordinality_injury_death_count').between(limit_start, limit_end)) \
                .withColumn("VEH_MAKE_ID_largest_injuries", col('VEH_MAKE_ID')) \
                .select('VEH_MAKE_ID_largest_injuries')


        except ValueError as ve:
            print(f"ValueError: {ve}")
            df_vehicle_wise_injury_death_count=self.spark.createDataFrame([(str(ve),)], ["issues_presists"])

        except Exception as e:
            print(f"Exception: {e}")
            df_vehicle_wise_injury_death_count=self.spark.createDataFrame([(str(e),)], ["issues_presists"])
        finally:
            return df_vehicle_wise_injury_death_count

    def get_top_ethnic_groups_per_body_style(self) -> DataFrame:
        """
        Returns a DataFrame with the top ethnic user group for each unique body style involved in crashes.

        Returns:
            DataFrame: A DataFrame with columns 'VEH_BODY_STYL_ID' and 'PRSN_ETHNICITY_ID'.
        """
        try:
            # filter to exclude while selecting data in their groups
            common_na_filter = ['NA', 'OTHER', 'UNKNOWN', 'OTHER  (EXPLAIN IN NARRATIVE)']

            # filtering the ethnic details data
            ethinic_details_person = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'PrimaryPerson').select(['PRSN_ETHNICITY_ID', 'CRASH_ID', 'UNIT_NBR']).filter(~col('PRSN_ETHNICITY_ID').isin(common_na_filter))

            # filtering the vehicle body style details data
            vechile_body_style = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'Unit').select(['VEH_BODY_STYL_ID', 'CRASH_ID', 'UNIT_NBR']).filter(~col('VEH_BODY_STYL_ID').isin(common_na_filter))

            # ethnic groups per each vehicle body style
            vechile_body_style_ethinicity_counts = vechile_body_style.join(ethinic_details_person, ['CRASH_ID', 'UNIT_NBR'], 'inner') \
                .groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID') \
                .agg(count(col('PRSN_ETHNICITY_ID')).alias('count_of_ethnicity'))

            # rank window of the ethnic group for each vehicle body style
            window = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col("count_of_ethnicity").desc())

            # picking the top ethnic group
            vechile_wise_top_ethinic_category = vechile_body_style_ethinicity_counts.withColumn("ethnicity_rank", rank().over(window)) \
                .filter(col('ethnicity_rank') == 1) \
                .select(['VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID'])


        except Exception as e:
            # Exception handling
            print(f"An error occurred: {str(e)}")
            vechile_wise_top_ethinic_category=self.spark.createDataFrame([(str(e),)], ["issues_presists"])
        finally:
            return vechile_wise_top_ethinic_category

    def get_top_zipcodes_with_alcohol_crashes(self)-> DataFrame:
        """
        Returns a DataFrame containing the top 5 zip codes with the highest number of crashes
        where alcohol is the contributing factor to the crash.
        """
        try:

            # Selecting crashed cars as body style
            crashed_cars_detail = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'Unit') \
                .select(['CRASH_ID', 'UNIT_NBR', 'VEH_BODY_STYL_ID']) \
                .filter(trim(col('VEH_BODY_STYL_ID')).rlike('PASSENGER\\s*CAR'))

            # Selecting entries where alcohol consumption result is positive and its zip code
            crash_alcohol_zipcode_details = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'PrimaryPerson') \
                .filter(col('PRSN_ALC_RSLT_ID') == 'Positive') \
                .filter(col('DRVR_ZIP') != 'NULL') \
                .select(['CRASH_ID', 'UNIT_NBR', 'PRSN_ALC_RSLT_ID', 'DRVR_ZIP'])

            # Rank window on the total number of crashes
            window = Window.orderBy(col("num_of_crash").desc())

            # Pulling up the crash zip code details and vehicle type and picking the top 5 zip codes with more crashes
            crash_alcohol_zipcode_zipcodes = crash_alcohol_zipcode_details.join(crashed_cars_detail, ['CRASH_ID', 'UNIT_NBR'], 'inner') \
                .groupBy('DRVR_ZIP') \
                .agg(count(col('CRASH_ID')).alias('num_of_crash')) \
                .withColumn("zip_crash_frequency_rank", row_number().over(window)) \
                .filter(col('zip_crash_frequency_rank') <= 5) \
                .select(['DRVR_ZIP'])


        except Exception as e:
            # Handle exceptions here
            print(f"An error occurred: {str(e)}")
            crash_alcohol_zipcode_zipcodes=self.spark.createDataFrame([(str(e),)], ["issues_presists"])
        finally:
            return crash_alcohol_zipcode_zipcodes

    def analyze_crash_data(self) -> DataFrame:
        """
        Analyzes crash data and returns a DataFrame.

        Returns:
            DataFrame: The analyzed crash data.
        """
        data = [Row(count_analyze_crash_data='not_able_to_convert')]
        try:
            # SELECTING CRASH ID where damaged property is present
            damage_crash_id = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'Damages').select('CRASH_ID', 'DAMAGED_PROPERTY') \
                .filter(~upper(col('DAMAGED_PROPERTY')).rlike('NONE')).select('CRASH_ID').distinct()
            damage_crash_id = [i[0] for i in damage_crash_id.collect()]

            # Selecting insurance where it's availed
            insurance_on = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'Charges').select(['CHARGE', 'CRASH_ID', 'UNIT_NBR']) \
                .filter(((upper(col('CHARGE')) == ('INSURANCE')) | (upper(col('CHARGE')).rlike('ON\\s*INSURANCE')) |
                         (upper(col('CHARGE')).rlike("DRIVER'S\\s*LICENSE\\s*INSURANCE"))) &
                        ((~upper(col('CHARGE')).rlike('FAIL')) & (~upper(col('CHARGE')).rlike('NO')) &
                         (~upper(col('CHARGE')).rlike('EXCLUDED')))).select(['CRASH_ID', 'UNIT_NBR'])

            # Crashes where damage level > 4 and selecting the crash_id and unit_nbr
            crshid_unitnbr_damage_level = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'Unit').select(['CRASH_ID', 'UNIT_NBR', 'VEH_DMAG_SCL_1_ID',
                                                                         'VEH_DMAG_SCL_2_ID']) \
                .withColumn('EH_DMAG_SCL_damage_level1',
                            regexp_extract(col('VEH_DMAG_SCL_1_ID'), r'DAMAGED (\d)', 1).cast(IntegerType())) \
                .withColumn('EH_DMAG_SCL_damage_level2',
                            regexp_extract(col('VEH_DMAG_SCL_2_ID'), r'DAMAGED (\d)', 1).cast(IntegerType())) \
                .filter(col('EH_DMAG_SCL_damage_level1').isNotNull()) \
                .filter(col('EH_DMAG_SCL_damage_level2').isNotNull()) \
                .filter(col('EH_DMAG_SCL_damage_level1') > 4) \
                .filter(col('EH_DMAG_SCL_damage_level2') > 4) \
                .select(['CRASH_ID', 'UNIT_NBR'])

            # Joins of all the data like insurance, damage level, and insurance details
            temp_damage_level_filter = crshid_unitnbr_damage_level.filter(~col('CRASH_ID').isin(damage_crash_id))
            result = temp_damage_level_filter.join(insurance_on,
                                                   ((temp_damage_level_filter['CRASH_ID'] != insurance_on['CRASH_ID']) &
                                                    (temp_damage_level_filter['UNIT_NBR'] != insurance_on[
                                                        'UNIT_NBR'])), 'inner') \
                .select(temp_damage_level_filter['CRASH_ID'].alias('CRASH_ID_distinct')).distinct()
            data = [Row(count_analyze_crash_data=result.count())]


        except Exception as e:
            print(f"An error occurred: {str(e)}")
        finally:
            return self.spark.createDataFrame(data)

    def get_top_5_vehicle_makes(self) -> DataFrame:
        """
        Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences,
        has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences.

        Returns:
        - DataFrame: Top 5 Vehicle Makes with the given conditions.
        """
        try:
            # top 10 used vehicle colors
            top_10_used_vehicle_colours = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'Unit').filter(col('VEH_COLOR_ID') != 'NA').groupBy('VEH_COLOR_ID').agg(count(col('VEH_COLOR_ID')).alias('count_of_color')).withColumn('color_rank', rank().over(Window.orderBy(desc('count_of_color'))))\
                .filter(col('color_rank') <= 10).select('VEH_COLOR_ID')
            top_10_colours = [i[0] for i in top_10_used_vehicle_colours.collect()]

            # car licensed with the Top 25 states with highest number of offenses
            top_25_offence_states = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'Unit').filter(col('VEH_LIC_STATE_ID') != 'NA').groupBy('VEH_LIC_STATE_ID').agg(count(col('VEH_LIC_STATE_ID')).alias('count_of_offence'))\
                .withColumn('offence_rank', rank().over(Window.orderBy(desc('count_of_offence'))))\
                .filter(col('offence_rank') <= 25).select('VEH_LIC_STATE_ID')
            top_25_offensive_states = [i[0] for i in top_25_offence_states.collect()]

            # records filtering of top colors and top_25 offensive states
            vech_top_colors_offenstates = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'Unit').select('CRASH_ID', 'UNIT_NBR', 'VEH_MAKE_ID', 'VEH_COLOR_ID')\
                .filter(col('VEH_COLOR_ID').isin(top_10_colours)).\
                filter(col('VEH_LIC_STATE_ID').isin(top_25_offensive_states)).select('CRASH_ID', 'UNIT_NBR', 'VEH_MAKE_ID')

            # drivers are charged with speeding related offenses
            speed_related_charges = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'Charges').filter(lower(col('CHARGE')).rlike('speed')).select('CRASH_ID', 'UNIT_NBR')

            # licensed Drivers
            liscensed_members = self.load_dataobj.load_dataframe(self.spark,self.driver_obj,'PrimaryPerson').filter((col('DRVR_LIC_STATE_ID') != 'NA') & (col('DRVR_LIC_STATE_ID') != 'UNLICENSED')).select('CRASH_ID', 'UNIT_NBR')

            # top 5 makes with given condition
            vechile_make_final_rank = vech_top_colors_offenstates.join(speed_related_charges, ['CRASH_ID', 'UNIT_NBR'], 'inner')\
                .join(liscensed_members, ['CRASH_ID', 'UNIT_NBR'], 'inner')\
                .groupBy('VEH_MAKE_ID')\
                .agg(count(col('VEH_MAKE_ID')).alias('count_of_vech_make'))\
                .withColumn('vech_make_rank', rank().over(Window.orderBy(desc('count_of_vech_make'))))\
                .filter(col('vech_make_rank') <= 5).select('VEH_MAKE_ID')

        except Exception as e:
            print(f"An error occurred: {str(e)}")
            vechile_make_final_rank=self.spark.createDataFrame([(str(e),)], ["issues_presists"])
        finally:
            return vechile_make_final_rank