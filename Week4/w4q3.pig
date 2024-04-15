-- Load ratings data
ratings = LOAD 'ratings.data' USING PigStorage('\t') AS (userId:int, movieId:int, rating:int, timestamp:long);

-- Load movies data
movies = LOAD 'movies.item' USING PigStorage('|') AS (movieId:int, movieTitle:chararray, date:chararray, link:chararray);

-- Group ratings by movieId and count them
grouped_ratings = GROUP ratings BY movieId;
ratings_count = FOREACH grouped_ratings GENERATE group AS movieId, COUNT(ratings) AS count;

-- Join the ratings count with movies to get movie titles
ratings_with_titles = JOIN ratings_count BY movieId, movies BY movieId;

-- Project the movie title and its count, ensuring correct field projection
final_output = FOREACH ratings_with_titles GENERATE movies::movieTitle AS movieTitle, ratings_count::count AS count;

-- Order by count descending to get most popular movies at the top
ordered_movies = ORDER final_output BY count DESC;

-- Limit to get the top 1 most popular movie
most_popular_movie = LIMIT ordered_movies 1;

-- Output the result
STORE most_popular_movie INTO 'most_popular_movie' USING PigStorage(',');
STORE final_output INTO 'output_with_titles' USING PigStorage(',');
