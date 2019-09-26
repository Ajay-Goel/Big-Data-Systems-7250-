chars=( {a..z} )
i=0
for filename in `hadoop fs -ls /Assignment3/NYSE2 | awk '{print $NF}' | grep .csv$ | tr '\n' ' '`
do 
	echo $filename;
	hadoop jar /Users/ajaygoel/.m2/repository/com/ajay/mr/Assignment3_6/1.0-SNAPSHOT/Assignment3_6-1.0-SNAPSHOT.jar com.ajay.mr.assignment3_6.NYSEMR $filename /Assignment3_6/"${chars[i++]}"
done
