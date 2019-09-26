chars=( {a..z} )
i=0
for filename in `hadoop fs -ls /Assignment4/NYSE | awk '{print $NF}' | grep .csv$ | tr '\n' ' '`
do 
	echo $filename;
	hadoop jar /Users/ajaygoel/.m2/repository/com/ajay/mr/Lab5/1.0-SNAPSHOT/Lab5-1.0-SNAPSHOT.jar part2.Driver $filename /Assignment4/Result/"${chars[i++]}"
done


