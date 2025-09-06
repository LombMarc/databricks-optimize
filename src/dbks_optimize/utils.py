import logging
from pyspark.sql import SparkSession

class BaseClass:
    def __init__(self, spark_session : SparkSession, object_name : str, force_partition_on_col : str = None) -> None:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        self.logger = logger
        self.spark_session = spark_session
        self.object_name = object_name
        self.force_partition_on_col = force_partition_on_col
        self.database_object_type()
        
    def database_object_type(self):
        if self.object_name.count('.') == 0:
            if catalog_exists(self.spark_session, self.object_name):
                self.object_type = "catalog"
            else:
                raise ValueError(f"{self.object_name} catalog does not exist.")
            self.object_type = "catalog"
        elif self.object_name.count('.') == 1:
            if schema_exists(self.spark_session, self.object_name):
                self.object_type = "schema"
            else:
                raise ValueError(f"{self.object_name} schema does not exist.")
        elif self.object_name.count('.') == 2:
            if table_exists(self.spark_session, self.object_name):
                self.object_type = "table"
            else:
                raise ValueError(f"{self.object_name} table does not exist.")
        else:
            raise ValueError("Could not identify object type.")
    
    def check_db_available(self):
        pass
    
    def check_table_available(self):
        pass
    
    def compute_table_statistics(self):
        pass
    
    def generate_optimize_statement(self):
        pass

    def generate_partition_statement(self):
        pass

    def pre_optimization(self):
        pass


def catalog_exists(spark_session, object_name):
    catalogs = spark_session.sql("SHOW CATALOGS").collect()
    catalog_list = [row.catalog for row in catalogs]
    return object_name in catalog_list

def schema_exists(spark_session, object_name):
    try:
        spark_session.sql(f"DESCRIBE SCHEMA {object_name}")
        return True
    except:
        return False

def table_exists(spark_session, object_name):
    try:
        spark_session.sql(f"DESCRIBE TABLE {object_name}")
        return True
    except:
        return False

