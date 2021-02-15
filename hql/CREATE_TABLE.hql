CREATE DATABASE IF NOT EXISTS Jet_analysis;

-- Create external table
CREATE EXTERNAL TABLE `Jet_analysis.movie_ratings`(
  `reviewerID` string,
  `asin` string,
  `ratings` float,
  `rating_time` bigint,
  `rating_dt` string,
  `meta_asin` string,
  `title` string)
PARTITIONED BY (
  `year` int,
  `month` int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://<bucket_name>/nrajashyam/JE/output/batch_id=1613301962/';