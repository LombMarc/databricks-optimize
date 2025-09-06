import sys
import os
import pytest
from unittest.mock import Mock, MagicMock, patch


#handle library import
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
src_dir = os.path.join(project_root, 'src')
sys.path.insert(0, src_dir)

from dbks_optimize.optimizer import TableOptimizer


def test_table_optimizer_initialization():
    mock_spark = Mock()

    test_table = TableOptimizer(mock_spark, "catalog.schema.test_table")
    
    assert test_table.object_name == "catalog.schema.test_table"
    assert test_table.object_type == "table"
    

def test_schema_optimizer_initialization():
    mock_spark = Mock()

    test_schema = TableOptimizer(mock_spark, "catalog.schema")
    
    assert test_schema.object_name == "catalog.schema"
    assert test_schema.object_type == "schema"


class TestComputeTableStatistics:
    
    def setup_method(self):
        self.mock_spark = Mock()
        self.mock_logger = Mock()
        self.optimizer = TableOptimizer(self.mock_spark, "catalog.schema.table")
        self.optimizer.logger = self.mock_logger

        #reset mock count to have real count in tests
        self.mock_spark.reset_mock()
        self.mock_spark.read.reset_mock()
        self.mock_spark.sql.reset_mock()
        
    def test_compute_statistics_small_table(self):
        mock_table = Mock()
        mock_table.columns = ["col1", "col2", "col3"]
        
        mock_details = Mock()
        mock_details_select = Mock()
        mock_details_select.collect.return_value = [(500000000,)]  # 0.5GB 
        
        mock_details.select.return_value = mock_details_select
        
        #use side effect to handle each
        def mock_select(column_name):
            mock_col = Mock()
            if column_name == "col1":
                mock_col.distinct.return_value.count.return_value = 100  # cardinality
                mock_col.count.return_value = 1000  # total records
            elif column_name == "col2":
                mock_col.distinct.return_value.count.return_value = 50
                mock_col.count.return_value = 1000
            elif column_name == "col3":
                mock_col.distinct.return_value.count.return_value = 200  # highest cardinality
                mock_col.count.return_value = 1000
            return mock_col
        
        mock_table.select.side_effect = mock_select
        
        #mock/overwrite method of spark session for read and for sql (since these are called only once per execution)
        self.mock_spark.read.table.return_value = mock_table
        self.mock_spark.sql.return_value = mock_details
        
        #run the method with the overwritte method
        result = self.optimizer.compute_table_statistics()
        
        assert result['PARTITIONING']['IS_SUGGESTED'] == "No"
        assert result['ZORDERING_SUGGESTED_COLUMN'] == "col3"  # highest cardinality
        assert len(result['PARTITIONING']['COLUMNS']) == 0  # no partition cols for small table
        
        self.mock_spark.read.table.assert_called_once_with("catalog.schema.table")
        self.mock_spark.sql.assert_called_once_with("DESCRIBE DETAIL catalog.schema.table")

    def test_compute_statistics_large_table_no_optimal(self):
        mock_table = Mock()
        mock_table.columns = ["col1", "col2", "col3"]
        
        mock_details = Mock()
        mock_details_select = Mock()
        mock_details_select.collect.return_value = [(1500000000,)]  # 1.5GB (1,500,000,000 bytes)
        
        mock_details.select.return_value = mock_details_select
        
        #use side effect to handle each
        def mock_select(column_name):
            mock_col = Mock()
            if column_name == "col1":
                mock_col.distinct.return_value.count.return_value = 100  # cardinality
                mock_col.count.return_value = 1000  # total records
            elif column_name == "col2":
                mock_col.distinct.return_value.count.return_value = 50
                mock_col.count.return_value = 1000
            elif column_name == "col3":
                mock_col.distinct.return_value.count.return_value = 200  # highest cardinality
                mock_col.count.return_value = 1000
            return mock_col
        
        mock_table.select.side_effect = mock_select
        
        #mock/overwrite method of spark session for read and for sql (since these are called only once per execution)
        self.mock_spark.read.table.return_value = mock_table
        self.mock_spark.sql.return_value = mock_details
        
        #run the method with the overwritte method
        result = self.optimizer.compute_table_statistics()
        

        assert result['PARTITIONING']['IS_SUGGESTED'] == "Yes"
        assert result['ZORDERING_SUGGESTED_COLUMN'] == "col3" 
        assert result['PARTITIONING']['COLUMNS'] == ["col2"]  
        

        self.mock_spark.read.table.assert_called_once_with("catalog.schema.table")
        self.mock_spark.sql.assert_called_once_with("DESCRIBE DETAIL catalog.schema.table")
         
    def test_compute_statistics_large_table_optimal(self):
            mock_table = Mock()
            mock_table.columns = ["col1", "col2", "col3"]
            
            mock_details = Mock()
            mock_details_select = Mock()
            mock_details_select.collect.return_value = [(1500000000,)]  # 1.5GB 
            
            mock_details.select.return_value = mock_details_select
            
            #use side effect to handle each
            def mock_select(column_name):
                mock_col = Mock()
                if column_name == "col1":
                    mock_col.distinct.return_value.count.return_value = 12  # cardinality
                    mock_col.count.return_value = 1000  # total records
                elif column_name == "col2":
                    mock_col.distinct.return_value.count.return_value = 50
                    mock_col.count.return_value = 1000
                elif column_name == "col3":
                    mock_col.distinct.return_value.count.return_value = 200  # highest cardinality
                    mock_col.count.return_value = 1000
                return mock_col
            
            mock_table.select.side_effect = mock_select
            
            #mock/overwrite method of spark session for read and for sql (since these are called only once per execution)
            self.mock_spark.read.table.return_value = mock_table
            self.mock_spark.sql.return_value = mock_details
            
            #run the method with the overwritte method
            result = self.optimizer.compute_table_statistics()
            

            assert result['PARTITIONING']['IS_SUGGESTED'] == "Yes"
            assert result['ZORDERING_SUGGESTED_COLUMN'] == "col3" 
            assert result['PARTITIONING']['COLUMNS'] == ["col1"]  
            

            self.mock_spark.read.table.assert_called_once_with("catalog.schema.table")
            self.mock_spark.sql.assert_called_once_with("DESCRIBE DETAIL catalog.schema.table")
     
    def test_compute_statistics_small_table_with_suggestion(self):
        optimizer = TableOptimizer(self.mock_spark, "catalog.schema.table", 
                                  force_partition_on_col='col1')
        self.mock_spark.reset_mock()
        self.mock_spark.read.reset_mock()
        self.mock_spark.sql.reset_mock()
        mock_table = Mock()
        mock_table.columns = ["col1", "col2", "col3"]
        
        mock_details = Mock()
        mock_details_select = Mock()
        mock_details_select.collect.return_value = [(500000000,)]  # 0.5GB 
        
        mock_details.select.return_value = mock_details_select

        

        #use side effect to handle each
        def mock_select(column_name):
            mock_col = Mock()
            if column_name == "col1":
                mock_col.distinct.return_value.count.return_value = 100  # cardinality
                mock_col.count.return_value = 1000  # total records
            elif column_name == "col2":
                mock_col.distinct.return_value.count.return_value = 50
                mock_col.count.return_value = 1000
            elif column_name == "col3":
                mock_col.distinct.return_value.count.return_value = 200  # highest cardinality
                mock_col.count.return_value = 1000
            return mock_col
        
        mock_table.select.side_effect = mock_select
        
        #mock/overwrite method of spark session for read and for sql (since these are called only once per execution)
        self.mock_spark.read.table.return_value = mock_table
        self.mock_spark.sql.return_value = mock_details
        
        #run the method with the overwritte method
        result = optimizer.compute_table_statistics()
        
        # Verify the results for small table
        assert result['PARTITIONING']['IS_SUGGESTED'] == "No"
        assert result['ZORDERING_SUGGESTED_COLUMN'] == "col3"  # highest cardinality
        assert result['PARTITIONING']['COLUMNS'][0] == "col1"  # no partition cols for small table
        
        # Verify Spark calls were made
        self.mock_spark.read.table.assert_called_once_with("catalog.schema.table")
        self.mock_spark.sql.assert_called_once_with("DESCRIBE DETAIL catalog.schema.table")

