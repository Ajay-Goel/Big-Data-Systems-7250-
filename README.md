# Big Data Analysis using Airline Data set, 1987-2008 (Hadoop🐘, Java, Pig🐷, Hive🐝, HBase🐬, Amazon EMR, Python, Tableau📊, Neo4j🌴)
# Engineering of Big Data Systems

- Ajay Goel

![alt text](https://github.com/Ajay-Goel/Big-Data/blob/master/Project/Media/shutterstockhadoop_elephant_in_words%20(2).jpg)


## Introduction
#### Big Data Analysis
(Airline Dataset 1987 - 2008)
- Map Reduce (Hadoop)
- PIG
- HIVE
- Python (Sentimental Analysis)
- AWS EMR
- Tableau
- Neo4j


Table of Contents
1. About Dataset

Analysis Performed: 

2. Map – Reduction using Hadoop
- Bloom Filter: Source and Destination Test (Filter Pattern Algorithms) 
- Delay and Scheduled flights based on Year
- Busiest Airport in10 years & percentage using (Recommendation System)
- Number of flights visits each month (Counter Algorithm) 
- From each source, destination is indexed (Inverted Index Algorithm)
- Binning Algorithm: Creating bins on the bases of codes (Data Organisation Algorithm)
- Most Visited Destination (TOP 10 Algorithm)
- Carrieer and Source Join (Joins: Inner Join)
- Hierarchy Algorithm (XML Creation using Data Organization Pattern Algorithm)
- WORST 20 Flights (Filter Pattern Algorithm)
- Sampling using low scoring probability (Filter Pattern Algorithm)
- Finding unique sources
- Total Number of flights in each year (Min Max Tuple)
- Standard deviation, Mean Distance in each year
- Distributed Grep (Filter Pattern Algorithm)

3. PIG
- Create schema from the script Schema.pig
- Top 20 cities by total volume of flights
- Busy Routes
- Proportion of Flights Delayed
- Carrier Popularity

4. HIVE
- Schema creation
- Flights that started late but reached on time
- Flights that travel less than 1000 miles
- Count of flights for each Carrier

5. Sentiment Analysis
- Analyzing Tweets of POTUS from twitter using vader lexicon dictionary

6. AMAZON EMR
- Top 10 Busiest Airport with percentage
Business Visualization

7. TABLEAU
- Dashboard 1
- Dashboard 2

8. Graph Database
- Neo4j

About Dataset:
The Dataset of the flight records in USA is available on http://statcomputing.
org/dataexpo/2009/the-data.html. The data is available from year 1987 to 2008.
It has many columns which can be helpful in Map Reduction analysis. Once it is done, more
analysis on the basis of aggregation or various important points can be done using Pig, Hive,
HBase.

Variable descriptions
Col.
No. Name Description
1 Year 1987-2008
2 Month 12-Jan
3 DayofMonth 31-Jan
4 DayOfWeek 1 (Monday) - 7 (Sunday)
5 DepTime actual departure time (local, hhmm)
6 CRSDepTime scheduled departure time (local, hhmm)
7 ArrTime actual arrival time (local, hhmm)
8 CRSArrTime scheduled arrival time (local, hhmm)
9 UniqueCarrier unique carrier code
10 FlightNum flight number
11 TailNum plane tail number
12 ActualElapsedTime in minutes
13 CRSElapsedTime in minutes
14 AirTime in minutes
15 ArrDelay arrival delay, in minutes
16 DepDelay departure delay, in minutes
17 Origin origin IATA airport code
18 Dest destination IATA airport code
19 Distance in miles
20 TaxiIn taxi in time, in minutes
21 TaxiOut taxi out time in minutes
22 Cancelled was the flight cancelled?
Goel, Ajay: Nu Id (001897443)
23 CancellationCode
Reason for cancellation (A = carrier, B =
weather, C = NAS, D = security)
24 Diverted 1 = yes, 0 = no
25 CarrierDelay in minutes
26 WeatherDelay in minutes
27 NASDelay in minutes
28 SecurityDelay in minutes
29 LateAircraftDelay in minutes

/***
| __Animals__ | __Sports__ | __Fruits__ |
|-------------|------------|------------|
| Cat         | Soccer     | Apple      |
| Dog         | Basketball | Orange     |
***/


A short presentation to get the understating of working of the project with code and output snippets in file named: ProjectReport_Goel_Ajay_001897443_bkp6.pdf
