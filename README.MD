# Databricks optimize utility
Library to generate more easily optimize statement based on existing data.

## Build from source
Build the wheel file is enough to run
```
python -m build --wheel
```

## Table optimizer
This object can be initialized with a table object name (with three level namespace "catalog.schema.table"), the method pre_optimization will run some statistics to identify the best column to run z-order (column with highest cardinality) and to re-partition (this depends on the size of the table and the size of the generated files partition). This is used to define and print the optimization query. Then to run the optimization itself we have two additional method, run_optimize and run_partition, these two can be executed even on their own without the need to run the pre_optimization, in this case the method will run the statistics and the generation separately.


```
from dbks_optimize.optimizer import TableOptimizer

opt = TableOptimizer(spark,"testing.bakehouse.sales_customers",force_partition_on_col='customerID')

opt.pre_optimization() #print out the optimization statement

opt.run_optimizer() #execute optimize on the table

opt.run_partition() #execute partitioning query (it clone i a new table and overwrite the existing one)
```

## Schema optimizer
This object leverage the existing class for single table object and iterate over list of table available computing table statistics.
pre_optimize is used to generate the optimize statement, while to run real optimization we can use run_db_optimization.

```
from dbks_optimize.optimizer import SchemaOptimizer

opt = SchemaOptimizer(spark,'testing.bakehouse')

opt.pre_optimization()

opt.run_db_optimization()
```

## Catalog optimizer
Using the same logic iterate over all schemas and for each on on all tables to generate the statement.
```
from dbks_optimize.optimizer import CatalogOptimizer

opt = CatalogOptimizer(spark,'testing')

opt.pre_optimization()

opt.run_catalog_optimization()
```

