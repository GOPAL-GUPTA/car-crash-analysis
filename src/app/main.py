import time
from logger import get_logger
from config import get_config
from spark_session import create_spark_session
from modules.reader import DataReader
from modules.writer import DataWriter
from modules.transformer import DataTransformer


def main():
    # logger
    logger = get_logger("CarCrashAnalysis", log_file=f"logs/app_{time.strftime('%Y%m%d_%H%M%S')}.log")
    logger.info("Starting Car Crash Analysis Application")

    try: 
        # Load Configuration
        config = get_config()

        # Initialize Spark session
        spark = create_spark_session()
        logger.info("Spark session created successfully")

        # Read Data
        data_reader = DataReader(spark, config)
        charges_df = data_reader.read_csv("Charges")
        damages_df = data_reader.read_csv("Damages")
        endorse_df = data_reader.read_csv("Endorse")
        primary_person_df = data_reader.read_csv("Primary_Person")
        restrict_df = data_reader.read_csv("Restrict")
        units_df = data_reader.read_csv("Units")
        logger.info("Data successfully read from source")
        
        # Write output 
        writer = DataWriter(config)

        # DataTransformer to Perform Analysis
        transformer = DataTransformer()

        # Analytics 1: 
        
        # Filter for males
        male_df = transformer.filter_male_fatalities(primary_person_df)
        
        crash_count_df = transformer.find_crashes_with_male_fatalities(male_df)
        writer.write_to_csv(crash_count_df,"analytics_result_1")
        logger.info(f"Analytics Phase 1 completed successfully. Results written to Output Folder")

        # Analytics 2: 
        two_wheeler_count = transformer.count_two_wheelers(units_df)
        writer.write_to_csv(two_wheeler_count,"analytics_result_2")
        logger.info(f"Analytics Phase 2 completed successfully. Results written to Output Folder")

        # Analytics 3: 
        top_5_vehicle_makes = transformer.get_top_5_vehicle_makes(primary_person_df, units_df)

        # Log the result
        writer.write_to_csv(top_5_vehicle_makes,"analytics_result_3")
        logger.info(f"Analytics Phase 3 completed successfully. Results written to Output Folder")

        # Analytics 4: 
        valid_license_hit_and_run_count = transformer.count_valid_license_hit_and_run(primary_person_df, units_df)

        # Log the result
        writer.write_to_csv(valid_license_hit_and_run_count,"analytics_result_4")
        logger.info(f"Analytics Phase 4 completed successfully. Results written to Output Folder")

        # Analytics 5: 
        top_state = transformer.get_state_with_highest_accidents_no_females(primary_person_df)

        # Log the result
        writer.write_to_csv(top_state,"analytics_result_5")
        logger.info(f"Analytics Phase 5 completed successfully. Results written to Output Folder")

        # Analytics 6: 
        top_3rd_to_5th_vehicle_makes = transformer.get_top_3rd_to_5th_vehicle_makes(units_df)

        # Log the result
        writer.write_to_csv(top_3rd_to_5th_vehicle_makes,"analytics_result_6")
        logger.info(f"Analytics Phase 6 completed successfully. Results written to Output Folder")

        # Analytics 7: 
        top_ethnic_groups = transformer.get_top_ethnic_user_group_by_body_style(primary_person_df, units_df)        
        
        # Log the result
        writer.write_to_csv(top_ethnic_groups,"analytics_result_7")
        logger.info(f"Analytics Phase 7 completed successfully. Results written to Output Folder")

        # Analytics 8: 
        top_5_zip_codes = transformer.get_top_5_alcohol_related_crash_zip_codes(primary_person_df,units_df)

        # Log the result
        writer.write_to_csv(top_5_zip_codes,"analytics_result_8")
        logger.info(f"Analytics Phase 8 completed successfully. Results written to Output Folder")
        
         # Analytics 9: 
        crash_count = transformer.count_distinct_crashes_no_damage_high_level_insurance(units_df,damages_df)

        # Log the result
        writer.write_to_csv(crash_count,"analytics_result_9")
        logger.info(f"Analytics Phase 9 completed successfully. Results written to Output Folder")

        # Analytics 10: 
        top_5_vehicle_makes = transformer.get_top_5_vehicle_makes_driver_charged(primary_person_df, units_df, charges_df)

        # Log the result
        writer.write_to_csv(top_5_vehicle_makes,"analytics_result_10")
        logger.info(f"Analytics Phase 10 completed successfully. Results written to Output Folder")



    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
    finally:
        logger.info("Spark session stopped")
        logger.info("Car Crash Analysis Application finished successfully")


if __name__ == "__main__":
    
    main()



