package part7;



import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class MyReducer extends Reducer<IntWritable, SortedMapWritable,IntWritable,MedianSDWritable> {
    private MedianSDWritable msd = new MedianSDWritable();
    private TreeMap<Double,Long> outavgPrice = new TreeMap<>();

    @Override
    protected void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
        float sum=0;
        long totalStocks=0;
        outavgPrice.clear();
        msd.setMedian_Ratings(0);
        msd.setStandardDeviation_Ratings(0);

        for(SortedMapWritable res : values){
            for(Object e :res.entrySet()){
                Map.Entry<WritableComparable, Writable> entry = (Map.Entry<WritableComparable, Writable>) e;
                double stock = ((DoubleWritable)entry.getKey()).get();
                long count = ((LongWritable)entry.getValue()).get();
                totalStocks+=stock;
                sum+=stock*count;

                Long storedCount = outavgPrice.get(stock);
                if(storedCount==null){
                    outavgPrice.put(stock,count);
                } else {
                    outavgPrice.put(stock,storedCount+count);
                }
            }
            res.clear();
        }

        long medianIndex = totalStocks/2;
        long previosRating =0;
        long Rating =0;
        double preKey =0;

        for(Map.Entry<Double,Long>entry : outavgPrice.entrySet()){
            Rating = previosRating+entry.getValue();
            if(previosRating<=medianIndex && medianIndex<Rating){
                if(totalStocks%2==0 && previosRating ==medianIndex){
                    msd.setMedian_Ratings((float)(entry.getKey()+preKey)/2.0f);
                }
                else {
                    msd.setMedian_Ratings(entry.getKey());
                }
                break;
            }
            previosRating=Rating;
            preKey=entry.getKey();
        }

        //calculate sd
        float mean = sum/totalStocks;
        float sumOfSquares = 0.0f;
        for(Map.Entry<Double,Long>entry:outavgPrice.entrySet()){
            sumOfSquares+=(entry.getKey()-mean)*(entry.getKey()-mean)*entry.getValue();
        }
        msd.setStandardDeviation_Ratings((float)Math.sqrt(sumOfSquares/(totalStocks-1)));
        context.write(key,msd);
    }
}
