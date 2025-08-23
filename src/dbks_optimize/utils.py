import logging
from pyspark.sql import SparkSession
class BaseClass:
    def __init__(self, spark_session : SparkSession) -> None:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        self.logger = logger
        self.spark_session = spark_session
    
    def database_object_selection(self, object_name : str): 
        pass
    
    def compute_object_size(self):
        pass
    
    def check_table_available(self):
        pass
    
    def compute_cardinality(self):
        pass
    
    def estimate_partitions_size(self):
        pass

    def generate_optimize_statement(self):
        pass
    
    