import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType


class FileHelpers:
    def __init__(self):
        self.pwd = os.getcwd()
        path = "dataOut"
        if not os.path.exists(path):
            os.makedirs(path)

    def read_data(self, data_file, schema, sep):
        return spark.read.csv(path=os.path.join(self.pwd, 'dataIn/', data_file),
                              sep=sep,
                              schema=schema)

    def write_data_to_single_csv(self, df, file_name):
        path = "dataOut"
        if not os.path.exists(path):
            os.makedirs(path)
        df.coalesce(1).write.csv(path=os.path.join(self.pwd, 'dataOut/', file_name), header=True, mode='overwrite')

    def read_query(self, query_file_name):
        with open(os.path.join(self.pwd, 'queries/', query_file_name)) as f:
            return f.read()


class Schemas:
    ratings_schema = StructType([
        StructField("UserID", IntegerType(), False),
        StructField("MovieID", IntegerType(), False),
        StructField("Rating", StringType(), True),
        StructField("Timestamp", IntegerType(), True)])

    movies_schema = StructType([
        StructField("MovieID", IntegerType(), False),
        StructField("Title", StringType(), True),
        StructField("Genres", StringType(), True)])


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    fh = FileHelpers()

    df_ratings = fh.read_data('ratings.dat', Schemas.ratings_schema, "::")
    df_ratings = df_ratings.withColumn('Timestamp', F.from_unixtime('Timestamp').cast(TimestampType()))
    df_ratings.show()

    """New DataFrame for Movies with Min, Max, Avg."""
    df_ratings.createOrReplaceTempView("df_ratings")
    q1 = fh.read_query('rating_aggr.sql')
    df_ratings_aggr = spark.sql(q1)
    # df_ratings_aggr.show()

    """Based on the ask, this may be sufficient, but for avoidance of doubt..."""
    df_ratings_aggr.createOrReplaceTempView("df_ratings_aggr")
    q2 = fh.read_query('rating_aggr_full.sql')
    df_ratings_aggr_full = spark.sql(q2)
    # df_ratings_aggr_full.show()
    fh.write_data_to_single_csv(df_ratings_aggr_full, 'ratings_aggregations.csv')

    """User's Top 3 Movies"""
    df_movies = fh.read_data('movies.dat', Schemas.movies_schema, "::")
    df_movies.createOrReplaceTempView("df_movies")
    q3 = fh.read_query('rating_top3.sql')
    df_ratings_top3 = spark.sql(q3)
    # df_ratings_top3.show()

    """Based on the ask, this may be sufficient, but for avoidance of doubt..."""
    df_ratings_top3.createOrReplaceTempView('df_ratings_top3')
    q4 = fh.read_query('rating_top3_transposed.sql')
    df_ratings_top3_transposed = spark.sql(q4)
    # df_ratings_top3_transposed.show()
    fh.write_data_to_single_csv(df_ratings_aggr_full, 'user_preferences.csv')
