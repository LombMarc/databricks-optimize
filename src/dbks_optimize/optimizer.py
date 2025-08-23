from utils import BaseClass

class Optimizer(BaseClass):
    def compute_table_statistics(self):
        table = self.spark_session.read.table(self.object_name)
        columns = table.columns
        details = self.spark_session.sql(f"DESCRIBE DETAIL {self.object_name}")
        size = details.select("sizeInBytes").collect()[0][0]
        #check if partitioning is viable solution, not if we have small table (<1gb)
        if size < 1e9:
            sugg_partition = "No"
        else:
            sugg_partition = "Yes"
        #check cardinality and partition size for each column (suggested between 100 and 200 mb)
        cols_info = dict()
        max_card_col = columns[0]
        partition_cols = []
        if self.force_partition_on_col is not None:
            partition_cols.append(self.force_partition_on_col)

        for col in columns:
            temp = table.select(col)
            cardinality = temp.distinct().count()
            files_size = size/cardinality
            cols_info[col] = {"cardinality": cardinality, "records" : temp.count(), "files_size_partition":files_size}
            if cardinality> cols_info[max_card_col]["cardinality"]:
                max_card_col = col
            if  1.1e8<files_size<1.9e8:
                partition_cols.append(col)

        stats = {'PARTITIONING' : {
            'IS_SUGGESTED' : sugg_partition,
            'COLUMNS' : partition_cols
        },
        'ZORDERING_SUGGESTED_COLUMN': max_card_col,
        'COLUMNS_INFO': cols_info
        }
        self.table_statistic = stats

    def generate_optimize_statement(self):
        if self.table_statistic['PARTITIONING']['IS_SUGGESTED'] == 'Yes' or self.table_statistic['PARTITIONING']['COLUMNS'] != []:
            if len(self.table_statistic['PARTITIONING']['COLUMNS']) == 1:
                part_col = self.table_statistic['PARTITIONING']['COLUMNS'][0]
            else:
                max_card_col = self.table_statistic['PARTITIONING']['COLUMNS'][0]
                for col in self.table_statistic['PARTITIONING']['COLUMNS']:
                    if self.table_statistic['COLUMNS_INFO'][col]['cardinality'] > self.table_statistic['COLUMNS_INFO'][max_card_col]['cardinality']:
                        max_card_col = col
                part_col = max_card_col
            
            partition_statement = f"""
            -- 1. Create a temporary partitioned table with the data
            CREATE OR REPLACE TABLE testing.bakehouse.sales_customers_TEMP
            PARTITIONED BY (continent)
            TBLPROPERTIES (
            'delta.targetFileSize' = '134217728' -- 128MB target file size
            )
            AS SELECT * FROM testing.bakehouse.sales_customers;

            -- 3. Drop the original non-partitioned table
            DROP TABLE testing.bakehouse.sales_customers;

            -- 4. Rename the temporary table to the original name
            ALTER TABLE testing.bakehouse.sales_customers_TEMP RENAME TO testing.bakehouse.sales_customers;
"""

                
        
        
        z_order_condition = ""

        statement = f"OPTIMIZE {self.object_name} {where_condition} {z_order_condition}"
        return statement
    def table_pre_optimization(self):
        self.compute_table_statistics()

        

    def pre_optimization(self):
         