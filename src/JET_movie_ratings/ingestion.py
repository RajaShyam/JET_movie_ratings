import logging

from abc import abstractmethod
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, ArrayType, DoubleType
from pyspark.sql.functions import from_unixtime, month, year ,udf, regexp_replace, col


class HiveExecutionException(Exception):
    """Hive execution error """


class Ingestion():
    """
    Generic ingestion
    """
    def __init__(self, spark):
        """
        Initialize class variables
        :param spark:
        """
        self.spark = spark
        self.reg_ex = r"([a-z*]\s\'\d+\s)|(\\\')|(\d\'s)|([a-z]\'s\s)|(\'\d+[a-z])|([a-z+]\s+\"+)"
        self.ratings_columns = ['reviewerID', 'asin', 'ratings',
                                'rating_time', 'rating_dt', 'year',
                                'month', 'meta_asin', 'title']
        self.partition_cols = ['year', 'month']

    @staticmethod
    def get_ratings_schema():
        """
        get ratings schema
        :return:
        """
        ratings_schema = StructType([
            StructField("reviewerID", StringType(), True),
            StructField("asin", StringType(), True),
            StructField("ratings", FloatType(), True),
            StructField("rating_time", LongType(), True)
        ])
        return ratings_schema

    @staticmethod
    def get_movies_meta_schema():
        """
        get movies meta schema
        :return:
        """
        movies_meta_schema = StructType([
            StructField("asin", StringType()),
            StructField("categories", ArrayType(ArrayType(StringType(), True)), True),
            StructField("title", StringType(), True),
            StructField("price", DoubleType(), True)
        ])
        return movies_meta_schema

    def transform_ratings(self,
                          ratings_df):
        """
        perform transformations on ratings
        :param ratings_df:
        :return:
        """
        ratings_df = ratings_df.withColumn('rating_dt', from_unixtime(ratings_df.rating_time, format='yyyy-MM-dd'))
        ratings_df = ratings_df.withColumn('month', month(ratings_df.rating_dt))
        ratings_df = ratings_df.withColumn('year', year(ratings_df.rating_dt))
        ratings_df.printSchema()
        return ratings_df

    def add_hive_partitions(self, years):
        """
        Adds hive partitions to the table
        :param years:
        :return:
        """
        alter_stmt = "ALTER TABLE shyam.movie_ratings ADD IF NOT EXISTS PARTITION (year={year},month={month}) " \
        " LOCATION 's3://bucket-name/nrajashyam/JE/output/batch_id=1613301962/year={year}/month={month}/'"
        alter_drop_stmt = "ALTER TABLE shyam.movie_ratings DROP IF EXISTS PARTITION (year={year},month={month})"
        for year in years:
            for month in range(1, 13):
                try:
                    logger.info(alter_drop_stmt.format(year=year,
                                                       month=month))
                    logger.info(alter_stmt.format(year=year,
                                                     month=month))
                    self.spark.sql(alter_drop_stmt.format(year=year,
                                                       month=month))
                    self.spark.sql(alter_stmt.format(year=year,
                                                     month=month))
                except HiveExecutionException as err:
                    logger.info('Hive execution statement failed :- '+err)



    @abstractmethod
    def read(self):
        """
        read the source data
        :return:
        """
        pass

    @abstractmethod
    def save(self,
             ratings_meta_df):
        """
        save the data to storage layer
        :param ratings_meta_df:
        :return:
        """
        pass

    @abstractmethod
    def process(self):
        """
        process the movie ratings data
        :return:
        """
        pass


class MovieRatingsBatchIngestion(Ingestion):
    """
    Movie Ratings Batch Ingestion
    """
    def __init__(self, spark):
        """
        Initialize class variables
        :param spark: spark_session
        """
        super(MovieRatingsBatchIngestion, self).__init__(spark)

    def read(self):
        """
        read the data from source
        :return: movies_meta_data and ratings_data
        """
        movies_meta_df = (self.spark.read.format('json').
                          option("mode", "PERMISSIVE").
                          option("columnNameOfCorruptRecord", "_corrupt_record").
                          option('allowBackslashEscapingAnyCharacter','true').
                          load('hdfs:///tmp/JET/meta_movies/meta_Movies_and_TV.json.gz'))

        ratings_df = (self.spark.read.format('csv').
                      load('hdfs:///tmp/JET/movies_csv/ratings_Movies_and_TV.csv',
                           schema=self.get_ratings_schema()))
        return movies_meta_df, ratings_df

    def transform_meta_movies(self,
                              movies_meta_df):
        """
        perform transformations on meta movies data and clean up
        :param movies_meta_df: movies meta source data
        :return: final_movies_meta_df: cleansed movies meta data
        """
        movies_meta_df.cache()
        movies_meta_filtered_df = movies_meta_df.filter(movies_meta_df["_corrupt_record"].isNull()).select('asin',
                                                                                                           'categories',
                                                                                                           'title',
                                                                                                           'price')
        movies_meta_df.filter(movies_meta_df["_corrupt_record"].isNotNull()).count()
        #movies_meta_df.filter(movies_meta_df['_corrupt_record'].isNotNull()).select('_corrupt_record').show(5, False)
        corrupted_records_df = movies_meta_df.filter(movies_meta_df["_corrupt_record"].isNotNull())
        corrupted_records_df = corrupted_records_df.withColumn('_corrupt_record',
                                                               regexp_replace(corrupted_records_df['_corrupt_record'],
                                                                              self.reg_ex,
                                                                              ""))

        def transform_row(row):
            try:
                obj = eval(row)
            except Exception as e:
                logging.warning('Failure to parse record {0} with error {1}'.format(str(row), str(e.args)))
                record = ()
            else:
                asin = obj.get('asin')
                categories = obj.get('categories')
                title = obj.get('title')
                price = obj.get('price')
                record = (asin,
                          categories,
                          title,
                          price)
                return record

        transform_udf = udf(transform_row, self.get_movies_meta_schema())
        corrupted_records_df = corrupted_records_df.withColumn('corrected_data',
                                                               transform_udf(corrupted_records_df['_corrupt_record']))
        corrupted_records_df_formatted = corrupted_records_df.filter(col('corrected_data').isNotNull()).select(
            'corrected_data.*')
        final_movies_meta_df = movies_meta_filtered_df.unionByName(corrupted_records_df_formatted)
        final_movies_meta_df = final_movies_meta_df.withColumnRenamed('asin', 'meta_asin')
        return final_movies_meta_df

    def transform(self,
                  movies_meta_df,
                  ratings_df):
        """
        perform transformations on movies meta and ratings data
        :param movies_meta_df: movies meta source data
        :param ratings_df: ratings source data
        :return: ratings_meta_df: combined data of ratings and meta
        """
        ratings_df = self.transform_ratings(ratings_df)
        final_movies_meta_df = self.transform_meta_movies(movies_meta_df)
        ratings_meta_df = ratings_df.join(final_movies_meta_df, ratings_df.asin == final_movies_meta_df.meta_asin,
                                          'inner').select(*self.ratings_columns)
        ratings_meta_df.show(10, False)
        return ratings_meta_df

    def save(self,
             ratings_meta_df):
        """
        save the ratings and meta joined data to s3 and further analysis
        :param ratings_meta_df:
        :return:
        """
        ratings_meta_df.coalesce(1).write.parquet(path='s3://bucket-name/nrajashyam/JE/output/batch_id=1613301962',
                                      mode='overwrite',
                                      partitionBy=self.partition_cols)

    def process(self):
        """
        process movie ratings batch data
        :return:
        """
        movies_meta_df, ratings_df = self.read()
        ratings_meta_df = self.transform(movies_meta_df, ratings_df)
        self.save(ratings_meta_df)
        row_years = ratings_meta_df.select('year').distinct().collect()
        years = [r.year for r in row_years]
        logger.info(years)
        self.add_hive_partitions(years)


class MovieRatingsStreamingIngestion(Ingestion):
    """
    Movie Ratings streaming ingestion
    """

    def read(self):
        """"""
        pass

    def save(self):
        """"""
        pass

    def process(self):
        """"""
        pass

