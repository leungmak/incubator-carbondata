# CarbonData DataMap

## DataMap Implementation

DataMap can be created using following DDL

```
  CREATE DATAMAP [IF NOT EXISTS] datamap_name
  [ON TABLE main_table]
  USING "datamap_provider"
  DMPROPERTIES ('key'='value', ...)
  AS
    SELECT statement
```

Currently, there are 4 kinds of datamap. 

| DataMap provider | Description                      | DMPROPERTIES                                                 |
| ---------------- | -------------------------------- | ------------------------------------------------------------ |
| preaggregate     | single table pre-aggregate table | No DMPROPERTY is required                                    |
| timeseries       | time dimension rollup table.     | event_time, xx_granularity, please refer to [Timeseries DataMap](https://github.com/apache/carbondata/blob/master/docs/datamap/timeseries-datamap-guide.md) |
| mv               | multi-table pre-aggregate table, | No DMPROPERTY is required                                    |
| lucene           | lucene indexing                  | text_column                                                  |
|                  |                                  |                                                              |

## DataMap Management

There are two kind of management semantic for DataMaps:

####Incremental refresh

All datamap except MV datamap (preaggregate/timeseries/lucene) is in this category.

When user creates a datamap on the main table, system will immediately triger a datamap refresh automatically. It is triggered internaly by the system, and user does not need to issue REFRESH command

Afterwards, for every new data loading, system will immediatly trigger an incremental refresh on all related datamap created on the main table. The refresh is incremental based on Segment concept of Carbon.

If user perform data update, delete operation, system will return failure. (reject the operation)

If user drop the main table, the datamap will be dropped immediately too.

####Manual refresh

For MV datamap, since it involves multiple table, manual refresh is supported.

When user creates a mv datamap on multiple table, the datamap is created with status *disabled*, then user can issue REFRESH command to build the datamap. Every REFRESH command that user issued, system will trigger a full rebuild of the datamap. After rebuild is done, system will change datamap status to *enabled*, so that it can be used in query rewrite.

For every new data loading, data update, delete, the related datamap will be *disabled*.

If the main table is dropped by user, the related datamap will be dropped immediately.

In future, following feature should be supported in MV datamap (these currently is support in preaggregate datamap):

1. Incremental refresh, for single table case
2. Partitioning
3. Query rewrite on streaming table (union of groupby query on streaming segment and scan query on datamap table)

##DataMap Definition

There is no restriction on the datamap definition, the only restriction is that the main table need to be created before hand.

User can create datamap on managed table or unmanaged table (external table)

## Query rewrite using DataMap

How can user know whether datamap is used in certain query?

User can use EXPLAIN command to know, it will print out something like

```text
== CarbonData Profiler == 
Hit mv DataMap: datamap1
Scan Table: default.datamap1_table
+- filter: 
+- pruning by CG DataMap
+- all blocklets: 1
   skipped blocklets: 0
```

## DataMap Catalog

Currently, when user creates a datamap, system will store the datamap metadata in a configurable *system* folder in HDFS or S3.  

In this *system* folder, it contains:

- DataMapSchema file. It is a json file containing schema for one datamap. If user creates 100 datamaps (on different tables), there will be 100 files in *system* folder. 
- DataMapStatus file. Only one file, it is in json format, and each entry in the file represents for one datamap.

There is a DataMapCatalog interface to retrieve schema of all datamap, it can be used in optimizer to get the metadata of datamap. 

### Show DataMap

There is a SHOW DATAMAPS command, when this is issued, system will read all datamap from *system* folder and print all information on screen. The current information includes:

- DataMapName
- DataMapProviderName like mv, preaggreagte, timeseries, etc
- Associated Table

##Compaction on DataMap

This feature applies for preaggregate datamap only

Running Compaction command (`ALTER TABLE COMPACT`) on main table will **not automatically** compact the pre-aggregate tables created on the main table. User need to run Compaction command separately on each pre-aggregate table to compact them.

Compaction is an optional operation for pre-aggregate table. If compaction is performed on main table but not performed on pre-aggregate table, all queries still can benefit from pre-aggregate tables. To further improve the query performance, compaction on pre-aggregate tables can be triggered to merge the segments and files in the pre-aggregate tables. 

## Data Management on Main Table
In current implementation, data consistence need to be maintained for both main table and pre-aggregate tables. Once there is pre-aggregate table created on the main table, following command on the main table is not supported:
1. Data management command: `UPDATE/DELETE/DELETE SEGMENT`. 
2. Schema management command: `ALTER TABLE DROP COLUMN`, `ALTER TABLE CHANGE DATATYPE`, 
  `ALTER TABLE RENAME`. Note that adding a new column is supported, and for dropping columns and 
  change datatype command, CarbonData will check whether it will impact the pre-aggregate table, if 
   not, the operation is allowed, otherwise operation will be rejected by throwing exception.   
3. Partition management command: `ALTER TABLE ADD/DROP PARTITION`

However, there is still way to support these operations on main table, user can do as following:
1. Remove the pre-aggregate table by `DROP DATAMAP` command
2. Carry out the data management operation on main table
3. Create the pre-aggregate table again by `CREATE DATAMAP` command
  Basically, user can manually trigger the operation by re-building the datamap.




## Future requirement

1. Need to add interface in DataMapCatalog to retrieve datamap for given main table. This operation should be fast leveraging 1)signature of the given table. 2) some ML algorithm like decision tree, etc.
2. We need to add tenant information or different place for different tenant in the DataMap Catalog, so that we can retrieve datamap for given tenant.
3. Improve advisor capability like adding table size reduction estimation, query latency reduction estimation, by leveraging statistics in CBO