import os
import logging
import pandas as pd

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql import types
from pyspark.sql.functions import explode
import pyspark.sql.functions as func

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationEngine:
    """A movie recommendation engine
    """

    def __train_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
                  coldStartStrategy="drop")
        self.model = self.als.fit(self.ratingsdf)
        logger.info("ALS model built!")

    def get_top_ratings(self, user_id, movies_count):
        """Recommends up to movies_count top unrated movies to user_id
        """
        users = self.ratingsdf.select(self.als.getUserCol())
        users = users.filter(users.userId == user_id)
        userSubsetRecs = self.model.recommendForUserSubset(users, movies_count)
        userSubsetRecs = userSubsetRecs.withColumn("recommendations", explode("recommendations"))
        userSubsetRecs = userSubsetRecs.select(func.col('userId'),
                                               func.col('recommendations')['movieId'].alias('movieId'),
                                               func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                    drop('recommendations')
        userSubsetRecs = userSubsetRecs.drop('Rating')
        userSubsetRecs = userSubsetRecs.join(self.moviesdf, ("movieId"), 'inner')
        # userSubsetRecs.show()
        # userSubsetRecs.printSchema()
        userSubsetRecs = userSubsetRecs.toPandas()
        userSubsetRecs = userSubsetRecs.to_json()
        return userSubsetRecs

    def get_top_movie_recommend(self, movie_id, user_count):
        """Recommends up to movies_count top unrated movies to user_id
        """
        movies = self.ratingsdf.select(self.als.getItemCol())
        movies = movies.filter(movies.movieId == movie_id)
        movieSubsetRecs = self.model.recommendForItemSubset(movies, user_count)
        movieSubsetRecs = movieSubsetRecs.withColumn("recommendations", explode("recommendations"))
        movieSubsetRecs = movieSubsetRecs.select(func.col('movieId'),
                                                 func.col('recommendations')['userId'].alias('userId'),
                                                 func.col('recommendations')['Rating'].alias('Rating')).\
                                                                                        drop('recommendations')
        movieSubsetRecs = movieSubsetRecs.drop('Rating')
        movieSubsetRecs = movieSubsetRecs.join(self.moviesdf, ("movieId"), 'inner')
        # userSubsetRecs.show()
        # userSubsetRecs.printSchema()
        movieSubsetRecs = movieSubsetRecs.toPandas()
        movieSubsetRecs = movieSubsetRecs.to_json()
        return movieSubsetRecs

    def get_ratings_for_movie_ids(self, user_id, movie_id):
        """Given a user_id and a list of movie_ids, predict ratings for them
        """
        request = self.spark_session.createDataFrame([(user_id, movie_id)], ["userId", "movieId"])
        ratings = self.model.transform(request).collect()
        return ratings

    def add_ratings(self, user_id, movie_id, ratings_given):
        """Add additional movie ratings in the format (user_id, movie_id, rating)
        """
        # Convert ratings to an RDD
        new_ratings = self.spark_session.createDataFrame([(user_id, movie_id, ratings_given)],
                                                         ["userId", "movieId", "rating"])
        # Add new ratings to the existing ones
        self.ratingsdf = self.ratingsdf.union(new_ratings)
        # Re-train the ALS model with the new ratings
        self.__train_model()
        new_ratings = new_ratings.toPandas()
        new_ratings = new_ratings.to_json()
        return new_ratings

    def get_history(self, user_id):
        """Get rating history for a user
        """
        self.ratingsdf.createOrReplaceTempView("ratingsdata")
        user_history = self.spark_session.sql('SELECT userId, movieId, rating from ratingsdata where userId = "%s"' %user_id)
        user_history = user_history.join(self.moviesdf, ("movieId"), 'inner')
        user_history = user_history.toPandas()
        user_history = user_history.to_json()
        return user_history

    def __init__(self, spark_session, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        logger.info("Starting up the Recommendation Engine: ")
        self.spark_session = spark_session
        # Load ratings data for later use
        logger.info("Loading Ratings data...")
        ratings_file_path = os.path.join(dataset_path, 'training.csv')
        self.ratingsdf = spark_session.read.csv(ratings_file_path, header=True, inferSchema=True).na.drop()
        self.ratingsdf = self.ratingsdf.drop("timestamp")
        # Load movies data for later use
        logger.info("Loading Movies data...")
        movies_file_path = os.path.join(dataset_path, 'movies.csv')
        self.moviesdf = spark_session.read.csv(movies_file_path, header=True, inferSchema=True).na.drop()
        self.moviesdf = self.moviesdf.drop("genres")
        # Train the model
        self.__train_model()
