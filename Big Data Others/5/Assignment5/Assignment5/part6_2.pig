userData = LOAD '/Users/ajaygoel/Documents/Neu/BigData/Assignments/Assignment5/ml-1m/users.dat' USING PigStorage(':') AS (UserID,temp1,Gender,movieId,temp2,Genres);
genderGroup = GROUP userData BY Gender;
countGender = FOREACH genderGroup GENERATE group, COUNT(userData.UserID) as count;
dump countGender;