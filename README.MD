# Databricks optimize utility
Library to generate more easily optimize statement based on existing data.

## Table optimizer
This object can be initialized with a table object name (with three level namespace "catalog.schema.table"), the method pre_optimization will run some statistics to identify the best column to run z-order (column with highest cardinality) and to re-partition (this depends on the size of the table and the size of the generated files partition). This is used to define and print the optimization query. Then to run the optimization itself we have two additional method, run_optimize and run_partition, these two can be executed even on their own without the need to run the pre_optimization, in this case the method will run the statistics and the generation separately.