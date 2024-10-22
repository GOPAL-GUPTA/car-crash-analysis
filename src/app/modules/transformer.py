from pyspark.sql import DataFrame
from pyspark.sql.functions import col,sum as spark_sum,count,row_number,lower,regexp_extract,countDistinct
from pyspark.sql.window import Window

class DataTransformer:
    def filter_male_fatalities(self,primary_person_df):
        """
        Filters the Primary Person DataFrame to include only Male.
        
        Args:
            primary_person_df (DataFrame): The input DataFrame for Primary Person data.

        Returns:
            DataFrame: Filtered DataFrame containing only male records.
        """
        return primary_person_df.filter(col("PRSN_GNDR_ID") == "MALE")

    def find_crashes_with_male_fatalities(self, male_df):
        """
        Crashes where the number of male fatalities is greater than 2.
        
        Args:
            male_df (DataFrame): DataFrame filtered to include only male records.

        Returns:
            int: The count of crashes where more than 2 males were killed.
        """
        # Group by crash ID and sum the number of fatalities
        aggregated_df = male_df.groupBy("CRASH_ID").agg(spark_sum(col("DEATH_CNT")).alias("male_deaths"))
        
        # Filter where male fatalities are greater than 2
        result_df = aggregated_df.filter(col("male_deaths") > 2)
        
        # Count the number of crashes
        return result_df.agg(count("*").alias("crash_count"))
    
    def count_two_wheelers(self, units_df: DataFrame) -> int:
        """
        Counts the number of two-wheelers involved in crashes.
        
        Args:
            units_df (DataFrame): The DataFrame containing Units data.
        
        Returns:
            int: The count of two-wheelers.
        """
        two_wheeler_df = units_df.filter(
            col("VEH_BODY_STYL_ID").like("%MOTORCYCLE%") |
            col("VEH_BODY_STYL_ID").like("%SCOOTER%") |
            col("VEH_BODY_STYL_ID").like("%BIKE%")
        )

        return two_wheeler_df.agg(count("*").alias("two_wheeler_count"))


    def get_top_5_vehicle_makes(self, primary_person_df: DataFrame, units_df: DataFrame) -> list:
        """
        Determines the top 5 vehicle makes of cars where the driver died, and airbags did not deploy.
        
        Args:
            primary_person_df (DataFrame): The DataFrame containing Primary Person data.
            units_df (DataFrame): The DataFrame containing Units data.
        
        Returns:
            list: Top 5 vehicle makes.
        """
        # Filter for drivers who died
        driver_deaths_df = primary_person_df.filter(
            (col("PRSN_TYPE_ID") == "DRIVER") & (col("PRSN_INJRY_SEV_ID") == 'KILLED') & (col("PRSN_AIRBAG_ID") == 'NOT DEPLOYED')
        )

        # Filter Units data for cars
        cars_df = units_df.filter(
            (col("VEH_BODY_STYL_ID").like("%CAR%"))  # Ensuring it's a car
        )

        # Join the two filtered DataFrames on CRASH_ID
        joined_df = driver_deaths_df.join(cars_df, "CRASH_ID")

        # Group by VEH_MAKE_ID and count occurrences, then sort and get top 5
        top_makes_df = joined_df.groupBy("VEH_MAKE_ID") \
            .agg(count("*").alias("count")) \
            .orderBy(col("count").desc()) \
            .limit(5)
        
        top_vehicle_makes = top_makes_df.select(col("VEH_MAKE_ID").alias("top_5_vehicle_makes"))

        return top_vehicle_makes

    def count_valid_license_hit_and_run(self, primary_person_df: DataFrame, units_df: DataFrame) -> int:
        """
        Determines the number of vehicles with drivers having valid licenses involved in hit and run.
        
        Args:
            primary_person_df (DataFrame): The DataFrame containing Primary Person data.
            units_df (DataFrame): The DataFrame containing Units data.
        
        Returns:
            int: Count of vehicles matching the criteria.
        """
        #Filter for drivers with a valid license
        valid_license_drivers_df = primary_person_df.filter(
            (col("PRSN_TYPE_ID") == "DRIVER") & (col("DRVR_LIC_TYPE_ID").isin("DRIVER LICENSE","COMMERCIAL DRIVER LIC."))
        )

        # Filter Units data for hit and run vehicles
        hit_and_run_vehicles_df = units_df.filter(
            (col("VEH_HNR_FL") == "Y") &
            (col("VEH_BODY_STYL_ID").isNotNull())  # Ensure it's a vehicle
        )

        #Join the two filtered DataFrames on CRASH_ID
        joined_df = valid_license_drivers_df.join(hit_and_run_vehicles_df, "CRASH_ID")

        # return joined_df.count()
        return joined_df.agg(count("*").alias("valid_license_hit_and_run_count"))
    
    def get_state_with_highest_accidents_no_females(self, primary_person_df: DataFrame) -> str:
        """
        Determines the state with the highest number of accidents in which females are not involved.
        
        Args:
            primary_person_df (DataFrame): The DataFrame containing Primary Person data.
        
        Returns:
            str: The state with the highest number of accidents without female involvement.
        """
        #Filter out rows where the person is not female
        no_female_accidents_df = primary_person_df.filter(col("PRSN_GNDR_ID") != "FEMALE")

        #Group by driver's license state (DRVR_LIC_STATE_ID) and count
        top_state_df = no_female_accidents_df.groupBy("DRVR_LIC_STATE_ID") \
            .agg(count("*").alias("accident_count")) \
            .orderBy(col("accident_count").desc()) \
            .limit(1)

        top_state = top_state_df.select(col("DRVR_LIC_STATE_ID").alias("top_state_highest_number_accident"))
        
        return top_state
    
    def get_top_3rd_to_5th_vehicle_makes(self, units_df: DataFrame) -> list:
        """
        Determines the 3rd to 5th ranked VEH_MAKE_IDs that contribute to the largest number of injuries including death.
        
        Args:
            units_df (DataFrame): The DataFrame containing Units data.
        
        Returns:
            list: Top 3rd to 5th VEH_MAKE_IDs based on the highest injury and death count.
        """
        # Filter out rows where VEH_MAKE_ID is 'NA' or any non-informative value
        valid_units_df = units_df.filter(col("VEH_MAKE_ID") != "NA")

        #  Aggregate the total injuries and deaths by VEH_MAKE_ID
        injuries_df = valid_units_df.groupBy("VEH_MAKE_ID") \
            .agg(
                spark_sum(col("TOT_INJRY_CNT")).alias("total_injuries"),
                spark_sum(col("DEATH_CNT")).alias("total_deaths")
            )

        # Calculate the total impact (injuries + deaths)
        impact_df = injuries_df.withColumn("total_impact", col("total_injuries") + col("total_deaths"))

        # Sort by total impact in descending order
        sorted_impact_df = impact_df.orderBy(col("total_impact").desc())

        #  Select the 3rd to 5th ranked VEH_MAKE_IDs
        top_3rd_to_5th_df = sorted_impact_df.limit(5).subtract(sorted_impact_df.limit(2))


        top_3rd_to_5th = top_3rd_to_5th_df.select(col("VEH_MAKE_ID").alias("top_3rd_to_5th_VEH_MAKE_ID"))

        return top_3rd_to_5th

    def get_top_ethnic_user_group_by_body_style(self, primary_person_df: DataFrame, units_df: DataFrame) -> list:
        """
        Determines the top ethnic user group for each unique vehicle body style involved in crashes,
        excluding non-informative values.
        
        Args:
            primary_person_df (DataFrame): The DataFrame containing Primary Person data.
            units_df (DataFrame): The DataFrame containing Units data.
        
        Returns:
            DataFrame: DataFrame with top ethnic group for each vehicle body style.
        """
        # Filter out non-informative values in both tables
        valid_units_df = units_df.filter(~col("VEH_BODY_STYL_ID").isin("NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"))
        valid_primary_person_df = primary_person_df.filter(~col("PRSN_ETHNICITY_ID").isin("NA", "UNKNOWN"))

        # Join Primary Person with Units on CRASH_ID
        joined_df = valid_primary_person_df.join(valid_units_df, "CRASH_ID")

        # Group by VEH_BODY_STYL_ID and PRSN_ETHNICITY_ID, and count occurrences
        ethnic_count_df = joined_df.groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID") \
            .agg(count("*").alias("ethnic_count"))

        # Use Window function to rank ethnic groups by count for each body style
        window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("ethnic_count").desc())
        ranked_df = ethnic_count_df.withColumn("rank", row_number().over(window_spec))

        # Filter to get only the top-ranked ethnic group for each body style
        top_ethnic_groups_df = ranked_df.filter(col("rank") == 1).select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")

        # return top_ethnic_groups_df.collect()
        return top_ethnic_groups_df

    

    def get_top_5_alcohol_related_crash_zip_codes(self, primary_person_df: DataFrame, units_df: DataFrame) -> list:
        """
        Determines the Top 5 ZIP codes with the highest number of crashes where alcohol was a contributing factor.
        
        Args:
            primary_person_df (DataFrame): The DataFrame containing Primary Person data.
            units_df (DataFrame): The DataFrame containing Units data.
        
        Returns:
            list: Top 5 ZIP codes with the highest number of alcohol-related crashes.
        """
        #Filter the Units data for alcohol-related crashes
        alcohol_related_units_df = units_df.filter(
            (col("CONTRIB_FACTR_1_ID").contains("ALCOHOL")) | 
            (col("CONTRIB_FACTR_2_ID").contains("ALCOHOL"))
        )

        # Drop rows with null values in DRVR_ZIP
        primary_person_df = primary_person_df.dropna(subset=['DRVR_ZIP'])

        # Join with Primary Person data on CRASH_ID
        joined_df = primary_person_df.join(alcohol_related_units_df, "CRASH_ID")

        # Group by DRVR_ZIP and count the number of crashes
        zip_crash_count_df = joined_df.groupBy("DRVR_ZIP") \
            .agg(count("*").alias("crash_count"))

        #Sort by crash count in descending order and select the top 5
        top_5_zip_codes = zip_crash_count_df.orderBy(col("crash_count").desc()).limit(5)

        top_5_zip = top_5_zip_codes.select(col("DRVR_ZIP").alias("top_5_zip_codes"))

        return top_5_zip
    
    def count_distinct_crashes_no_damage_high_level_insurance(self, units_df: DataFrame, damages_df: DataFrame) -> int:
        """
        Counts the distinct Crash IDs where no damaged property was observed or DAMAGED_PROPERTY is null,
        damage level is above 4, and the car has insurance.
        
        Args:
            units_df (DataFrame): The DataFrame containing Units data.
            damages_df (DataFrame): The DataFrame containing Damages data.
        
        Returns:
            int: Count of distinct crash IDs matching the criteria.
        """
        # Filter Units DataFrame for high damage levels and insurance
        filtered_units_df = units_df.filter(
            ((regexp_extract(col("VEH_DMAG_SCL_1_ID"), r"(\d+)", 1).cast("int") > 4) |
            (regexp_extract(col("VEH_DMAG_SCL_2_ID"), r"(\d+)", 1).cast("int") > 4)) &
            (lower(col("FIN_RESP_TYPE_ID")).like("%insurance%"))
        )

        # Filter Damages DataFrame for no damaged property or null values
        filtered_damages_df = damages_df.filter(
            lower(col("DAMAGED_PROPERTY")).like("%no damage%"))

        #Join the filtered DataFrames on CRASH_ID
        joined_df = filtered_units_df.join(filtered_damages_df, "CRASH_ID")

        # Count distinct CRASH_IDs
        distinct_crash_count = joined_df.select("CRASH_ID").agg(countDistinct("CRASH_ID").alias("distinct_crash_count"))

        return distinct_crash_count
        
    def get_top_5_vehicle_makes_driver_charged(self, primary_person_df: DataFrame, units_df: DataFrame, charges_df: DataFrame) -> list:
        """
        Determines the Top 5 Vehicle Makes where drivers are charged with speeding-related offences,
        have licensed drivers, use top 10 vehicle colours, and are licensed with the Top 25 states with highest offences.
        
        Args:
            primary_person_df (DataFrame): The DataFrame containing Primary Person data.
            units_df (DataFrame): The DataFrame containing Units data.
            charges_df (DataFrame): The DataFrame containing Charges data.
        
        Returns:
            list: Top 5 Vehicle Makes meeting all the conditions.
        """
        # Identify records with speeding-related offences in Charges DataFrame
        speeding_df = charges_df.filter(lower(col("CHARGE")).like("%speeding%"))

        # Filter for specific licensed drivers from Primary Person DataFrame
        licensed_drivers_df = primary_person_df.filter(
            col("DRVR_LIC_TYPE_ID").isin("DRIVER LICENSE", "COMMERCIAL DRIVER LIC.")
        )

        # Determine the Top 10 vehicle colors in Units DataFrame
        top_10_colors_df = units_df.groupBy("VEH_COLOR_ID") \
            .agg(count("*").alias("color_count")) \
            .orderBy(col("color_count").desc()) \
            .limit(10)
        
        top_10_colors = [row["VEH_COLOR_ID"] for row in top_10_colors_df.collect()]

        # Identify the Top 25 states with highest number of offences
        top_25_states_df = units_df.groupBy("VEH_LIC_STATE_ID") \
            .agg(count("*").alias("state_offence_count")) \
            .orderBy(col("state_offence_count").desc()) \
            .limit(25)

        top_25_states = [row["VEH_LIC_STATE_ID"] for row in top_25_states_df.collect()]

        #  Join dataframes to filter and find Top 5 Vehicle Makes
        filtered_units_df = units_df.filter(
            col("VEH_COLOR_ID").isin(top_10_colors) &
            col("VEH_LIC_STATE_ID").isin(top_25_states)
        )
        
        # Join Primary Person, Units, and Speeding DataFrames
        joined_df = licensed_drivers_df.join(speeding_df, "CRASH_ID") \
                                       .join(filtered_units_df, "CRASH_ID")

        # Find Top 5 Vehicle Makes
        top_vehicle_makes_df = joined_df.groupBy("VEH_MAKE_ID") \
            .agg(count("*").alias("make_count")) \
            .orderBy(col("make_count").desc()) \
            .limit(5)
        
        top_vehicle_makes = top_vehicle_makes_df.select(col("VEH_MAKE_ID").alias("top_5_vehicle_makes_driver_charged"))
        
        return top_vehicle_makes
        
