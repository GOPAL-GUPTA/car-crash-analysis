import json 

def get_config():
    """
    Returns the configuration as a dictionary.
    """
    config = {
        "input_data_paths": {
            "Charges": "data/input/Charges_use.csv",
            "Damages": "data/input/Damages_use.csv",
            "Endorse": "data/input/Endorse_use.csv",
            "Primary_Person": "data/input/Primary_Person_use.csv",
            "Restrict": "data/input/Restrict_use.csv",
            "Units": "data/input/Units_use.csv"
        },
        "output_data_paths": {
            "analytics_result_1": "data/output/Analytics_result_1",
            "analytics_result_2": "data/output/Analytics_result_2",
            "analytics_result_3": "data/output/Analytics_result_3",
            "analytics_result_4": "data/output/Analytics_result_4",
            "analytics_result_5": "data/output/Analytics_result_5",
            "analytics_result_6": "data/output/Analytics_result_6",
            "analytics_result_7": "data/output/Analytics_result_7",
            "analytics_result_8": "data/output/Analytics_result_8",
            "analytics_result_9": "data/output/Analytics_result_9",
            "analytics_result_10": "data/output/Analytics_result_10"
        }
    }
    return config




    