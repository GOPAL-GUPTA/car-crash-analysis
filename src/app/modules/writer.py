from pyspark.sql import DataFrame
class DataWriter:
    def __init__(self, config):
        """
        Initialize the DataWriter with configuration.

        Args:
            config (dict): Configuration dictionary with output paths and settings.
        """
        self.config = config

    def write_to_csv(self, df: DataFrame, key, mode: str = "overwrite"):
        """
        Write a DataFrame to a CSV file.

        Args:
            df (DataFrame): Spark DataFrame to write.
            key (str): The key in the config that points to the output file path.
        """
        output_path = self.config["output_data_paths"].get(key)
        if not output_path:
            raise ValueError(f"No output path found for key '{key}' in configuration.")
        
        # Write the DataFrame to the specified path as CSV
        df.write.mode(mode).option("header",True).csv(output_path)

