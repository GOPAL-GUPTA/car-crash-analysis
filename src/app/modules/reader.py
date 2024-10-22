class DataReader:
    def __init__(self,spark,config):
        """
        Initialize the DataReader with a Spark session and configuration.

        Args:
            spark (SparkSession): The Spark session object.
            config (dict): Configuration dictionary with paths and other settings.
        """
        self.spark = spark
        self.config = config 

    def read_csv(self,key):
        """
        Reads a CSV file based on a key from the configuration.

        Args:
            key (str): The key in the config that points to the input file path.

        Returns:
            DataFrame: A Spark DataFrame containing the data from the CSV.
        """
        path = self.config["input_data_paths"].get(key)
        if not path:
            raise ValueError(f"No path found for key '{key}' in configuration.")
        
        # Read the CSV file using spark DataFrame API                      
        return self.spark.read.csv(path, inferSchema=True, header=True)

    
        

