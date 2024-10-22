import unittest
from pyspark.sql import SparkSession
from src.app.modules.reader import DataReader

class TestDataReader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("TestReader").getOrCreate()
        cls.config = {
            "input_data_paths": {
                "Primary_Person": "src/data/input/Primary_Person_use.csv"
            }
        }
        cls.data_reader = DataReader(cls.spark, cls.config)

    def test_read_csv(self):
        # Test reading Primary_Person dataset
        df = self.data_reader.read_csv("Primary_Person")
        
        # Basic checks to ensure the data is read correctly
        self.assertGreater(df.count(), 0, "Data should not be empty")
        self.assertIn("PRSN_GNDR_ID", df.columns, "Expected column 'PRSN_GNDR_ID' not found")
        self.assertIn("DEATH_CNT", df.columns, "Expected column 'DEATH_CNT' not found")

if __name__ == "__main__":
    unittest.main()
