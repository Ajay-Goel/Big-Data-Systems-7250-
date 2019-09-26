movieData = LOAD '/Users/ajaygoel/Documents/Neu/BigData/Assignments/Assignment5/ml-1m/movies.dat' USING PigStorage(':') AS (MovieID,temp1,Movie,temp2,Genres);
ratingsData = LOAD '/Users/ajaygoel/Documents/Neu/BigData/Assignments/Assignment5/ml-1m/ratings.dat' USING PigStorage(':') AS (UserID:int,temp1:chararray,MovieID:int,temp2:chararray,Rating:float,temp3:chararray,Timestamp:long);
groupRatings = GROUP ratingsData by MovieID;
sumRatings = FOREACH groupRatings GENERATE group as MovieID, COUNT(ratingsData.UserID) as count, SUM(ratingsData.Rating) as total;
ratingAvg = FOREACH sumRatings GENERATE MovieID, (total/count) as average;
top25_rating = LIMIT ratingAvg 25;
joinData = JOIN top25_rating BY MovieID, movieData BY MovieID;
top25_movies = LIMIT joinData 25;
dump top25_movies;