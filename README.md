# Compaction

The compaction process optimizes table reads by merging, partitioning, and sorting. The framework is built on AWS glue to handle full table reads, sort and partition the data. The framework can also be run on other Python frameworks if the data is small to medium in size. Below is the high level workflow.

1. Back up the original table in S3
2. Make use of the compaction framework.
3. Read the s3 parquet table and partition the data on partition col
4. Perform a sort within the partition.
5. Overwrite the data in the original table. Apply the row per partition if required.
6. Run the glue crawler after the write and grant the right permissions


##Compaction.py
This is  glue script which is configuration driven. 