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
1.1 Tools Used
Analysis Performed: 
2. Map – Reduction using Hadoop
2.1 Bloom Filter: Source and Destination Test (Filter Pattern Algorithms) 
2.2 Delay and Scheduled flights based on Year
2.3 Busiest Airport in10 years & percentage using (Recommendation System)
2.4 Number of flights visits each month (Counter Algorithm) 
2.5 From each source, destination is indexed (Inverted Index Algorithm)
2.6 Binning Algorithm: Creating bins on the bases of codes (Data Organisation Algorithm)
2.7 Most Visited Destination (TOP 10 Algorithm)
2.8 Carrieer and Source Join (Joins: Inner Join)
2.9 Hierarchy Algorithm (XML Creation using Data Organization Pattern Algorithm)
2.10 WORST 20 Flights (Filter Pattern Algorithm)
2.11 Sampling using low scoring probability (Filter Pattern Algorithm)
2.12 Finding unique sources
2.13 Total Number of flights in each year (Min Max Tuple)
2.14 Standard deviation, Mean Distance in each year
2.15 Distributed Grep (Filter Pattern Algorithm)
3. PIG
3.1 Create schema from the script Schema.pig
3.2 Top 20 cities by total volume of flights
3.3 Busy Routes
3.4 Proportion of Flights Delayed
3.5 Carrier Popularity
4. HIVE
4.1 Schema creation
4.2 Flights that started late but reached on time
4.3 Flights that travel less than 1000 miles
4.4 Count of flights for each Carrier
5. Sentiment Analysis
5.1 Analyzing Tweets of POTUS from twitter using vader lexicon dictionary
6. AMAZON EMR
6.1 Top 10 Busiest Airport with percentage
Business Visualization
7. TABLEAU
7.1 Dashboard 1
7.2 Dashboard 2
8. Graph Database
8.1 Neo4j
REFERENCES
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
