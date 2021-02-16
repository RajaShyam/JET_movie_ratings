import logging

from .base_test import PySparkTest
from argparse import Namespace
from JET_movie_ratings.ingestion import MovieRatingsBatchIngestion


class MovieRatingBatchIngestionTest(PySparkTest):
    """
    Tests for MovieRatingBatchIngestion
    """

    @classmethod
    def setUpClass(cls):
        """
        called before tests in an individual class are run
        :return:
        """
        logger = logging.getLogger('MovieRatingsBatchIngestion')
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
        cls.spark = cls.create_testing_pyspark_session()
        cls.movieRatingsBatchIngestion = MovieRatingsBatchIngestion(cls.spark, logger)

    def _read_data(self):
        """
        read data from json file
        :return: (data frame)
        """
        return self.spark.read.json('test_ratings.json')

    def test_transform(self):
        """
        Tests transform method, that does the ETL such as deriving difficulty of a recipe.
        checks weather actual data frame and expected data frame has same columns
        :return:
        """
        df = self._read_data()
        expected_df_columns = ['reviewerID', 'asin', 'ratings', 'rating_time']
        transform_df = self.movieRatingsBatchIngestion.transform_ratings(df)
        self.assertEqual(expected_df_columns, transform_df.columns, 'Actual and expected columns are equal')



