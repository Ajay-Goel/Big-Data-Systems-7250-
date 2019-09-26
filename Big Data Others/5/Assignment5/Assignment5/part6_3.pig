userData = LOAD '/Users/ajaygoel/Documents/Neu/BigData/Assignments/Assignment5/ml-1m/ratings.dat' USING PigStorage(':') AS (UserID:int,temp1:chararray,MovieID:int,temp2:chararray,Rating:float,temp3:chararray,Timestamp:long);
userGroup = GROUP userData BY UserID;
data = FOREACH userGroup GENERATE group, COUNT(userData.MovieID);
dump data;