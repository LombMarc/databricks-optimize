from dbks_optimize.utils import BaseClass

class TableOptimizer(BaseClass):
    def compute_table_statistics(self):
        self.logger.info(f"Start computing statistics for table {self.object_name}")
        table = self.spark_session.read.table(self.object_name)
        columns = table.columns
        details = self.spark_session.sql(f"DESCRIBE DETAIL {self.object_name}")
        size = details.select("sizeInBytes").collect()[0][0]
        #check if partitioning is viable solution, not if we have small table (<1gb)
        self.logger.info("Checking if partitioning is viable solution for performance optimization")
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
            
        self.logger.info("Check of cardinality and partition size for each column")
        for col in columns:
            temp = table.select(col)
            cardinality = temp.distinct().count()
            files_size = size/cardinality
            cols_info[col] = {"cardinality": cardinality, "records" : temp.count(), "files_size_partition":files_size}
            if cardinality> cols_info[max_card_col]["cardinality"]:
                max_card_col = col
            if  1.1e8<files_size<1.5e8:
                partition_cols.append(col)

        stats = {'PARTITIONING' : {
            'IS_SUGGESTED' : sugg_partition,
            'COLUMNS' : partition_cols
        },
        'ZORDERING_SUGGESTED_COLUMN': max_card_col,
        'COLUMNS_INFO': cols_info
        }
        self.logger.info("generating fina statistics for the object\n-------------------------------------------------")
        return stats

    def generate_optimize_statement(self, table_statistic):
        self.logger.info("Generating z-ordering statement")
        optimize_statement = f"OPTIMIZE {self.object_name} ZORDER BY {table_statistic['ZORDERING_SUGGESTED_COLUMN']}"

        
        return optimize_statement
    
    def generate_partition_statement(self,table_statistic):
        partition_statements = ()
        optimize_statement = None
        self.logger.info("Generating re-partitioning statements (Multiple query)")
        if table_statistic['PARTITIONING']['IS_SUGGESTED'] == 'Yes' or table_statistic['PARTITIONING']['COLUMNS'] != []:
            if len(table_statistic['PARTITIONING']['COLUMNS']) == 1:
                part_col = table_statistic['PARTITIONING']['COLUMNS'][0]
            else:
                max_card_col = table_statistic['PARTITIONING']['COLUMNS'][0]
                for col in table_statistic['PARTITIONING']['COLUMNS']:
                    if table_statistic['COLUMNS_INFO'][col]['cardinality'] > table_statistic['COLUMNS_INFO'][max_card_col]['cardinality']:
                        max_card_col = col
                part_col = max_card_col
            
            partition_statements = (f"""
            CREATE OR REPLACE TABLE {self.object_name}_TEMP
            PARTITIONED BY ({part_col})
            AS SELECT * FROM {self.object_name};""",
            f"""DROP TABLE {self.object_name};""",
            f"""ALTER TABLE {self.object_name}_TEMP RENAME TO {self.object_name};""")
            return partition_statements

    def pre_optimization(self, verbose = True):
        stats = self.compute_table_statistics()
        optimize_statement = self.generate_optimize_statement(stats)
        partition_statements = self.generate_partition_statement(stats)
        if verbose:
            print(f"----------OPTIMIZE STATEMENT----------\n{optimize_statement}\n\n----------PARTITION STATEMENT----------")
            
            for i, statement in enumerate(partition_statements):
                print(f"----------STEP {i}----------\n{statement}\n\n")
        
        self.optimize_statement = optimize_statement
        self.partition_statements = partition_statements
        
    def run_optimize(self):
        if self.optimize_statement is None:
            stats = self.compute_table_statistics()
            self.optimize_statement = self.generate_optimize_statement(stats)

        self.logger.info(f"Start optimizing table {self.object_name}")
        self.spark_session.sql(self.optimize_statement)

    def run_partition(self):
        if self.partition_statements is None:
            stats = self.compute_table_statistics()
            self.optimize_statement = self.generate_partition_statement(stats)
        self.logger.info(f"Start re-partitioning table {self.object_name}")
        for statement in self.partition_statements:
            self.spark_session.sql(statement)



    
         