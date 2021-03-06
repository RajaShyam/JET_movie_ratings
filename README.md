# Analysis of movie ratings data

### Perform the below steps for initial setup
1. Navigate to the path - `cd /mnt/tmp/;`
2. Clone this repository - `git clone https://github.com/RajaShyam/JET_movie_ratings.git`
3. Navigate to project directory - `cd JET_movie_ratings;`
4. Install the module - `sudo python3 setup.py clean build install`

Now the project initial scripts are installed as module. The below are the executable files - 

1. This executable file is responsible for setup of source data in hdfs - `/usr/local/bin/download_files`
2. This executable file is a trigger file to initiate the Movie Ratings ingestion process - `/usr/local/bin/movie_ratings_ingestion.py`

### Workflow orchestration tool - azkaban
This file helps to setup pipeline of jobs - `src/JET_movie_ratings/azkaban/project.py`

### How to build the azkaban job?
`azkaban build -c -p project.py -u username@domain-name:8081`

### Hive Table

Database - `Jet_analysis`
TableName - `movie_ratings`

|  **column Name** | **datatype**  | **Description**  |
|---|---|---|
| reviewerID  | string  | ID of the reviewer  |
|  asin | string  | ID of the product  |
| ratings  | float  | given rating by reviewer  |
| rating_time  | long  | datetime at which rating was given in epoch  |
| rating_dt  | date  | date of rating  |
| meta_asin | string  | ID of the product from meta movies   |
| title | string  | title of the movie  |
| year  | int  | year of the review   |
| month  | int  | month of the review   |


Create hive table using - `JET_movie_ratings/hql/CREATE_TABLE.hql`

### Download dataset

Download movie ratings and meta to local hdfs for further processing. Execute the script to setup the data - `sh /usr/local/bin/download_files`

### Batch Ingestion

`spark-submit --num-executors 20 --executor-cores 6 /usr/local/bin/movie_ratings_ingestion.py --job_name movie_ratings --action ingest`

* Ingestion job can be instantiated from azkaban using various configurations based on the different volume of data

* config is project property, we can pass different parameters - `spark-submit ${config} /usr/local/bin/movie_ratings_ingestion.py --job_name movie_ratings --action ingest`
    * large : `--num-executors 2 --executor-cores 2`
    * xlarge : `--num-executors 3 --executor-cores 3 --executor-memory 2G`
    * 2Xlarge : `--num-executors 5 --executor-cores 6 --executor-memory 3G`
    * 4Xlarge : `--num-executors 15 --executor-cores 6 --executor-memory 4G`
    * 8Xlarge : `--num-executors 20 --executor-cores 6 --executor-memory 5G`
    
### Streaming ingestion

`spark-submit --num-executors 20 --executor-cores 6 /usr/local/bin/movie_ratings_ingestion.py --job_name movie_ratings --action stream`

* micro-batch processing
* Streaming deduplication with watermarking

### Presto analysis

The dataset generated by ingestion job answers the below questions -

* What are the top 5 and bottom 5 movies in terms of overall average review ratings for a given month?
* For a given month, what are the 5 movies whose average monthly ratings increased the most compared with the previous month?



