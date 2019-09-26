echo Downloding file
curl -o /Users/ajaygoel/Documents/Neu/BigData/Assignments/Assignment3/nyse_zip.zip "http://msis.neu.edu/nyse/nyse.zip"
if [ "$?" -gt 0 ]
then
echo "Error downloading file a. Exiting"
exit
fi
echo "Unzipping the file"
unzip nyse_zip
if [ "$?" -gt 0 ]
then
echo "Error unzipping file"
exit
fi
echo "Unzipping the file"

fi
for filename in `ls NYSE | grep "NYSE_daily_prices_\([A-Z]\).csv"` ;
do
#	echo $filename;
	mongoimport -d bigdata_Ass3 -c nyse --type csv --file NYSE/$filename --headerline
        if [ $? -eq 0 ]; then
        	echo "$filename imported successfully!!!!"
        else
        	echo "Error while importing $filename!!!"
        	exit 1
        fi
done
echo "All files imported sucessfully!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"