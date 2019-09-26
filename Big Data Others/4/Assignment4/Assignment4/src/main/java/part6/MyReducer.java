package part6;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class MyReducer extends Reducer<Text,RatingWritable,Text,MedianSDWritable> {
    MedianSDWritable msdw = new MedianSDWritable();
    ArrayList<Float> al = new ArrayList<>();
    @Override
    protected void reduce(Text key, Iterable<RatingWritable> values, Context context) throws IOException, InterruptedException {
        float sum =0;
        float count=0;
        al.clear();
        msdw.setStandardDeviation_Ratings(0);

        for(RatingWritable rw : values) {
            sum+=rw.getRatings();
            count+=rw.getCount();
            al.add(rw.getRatings());
        }

        Collections.sort(al);

        if(count%2==0){
            msdw.setMedian_Ratings((al.get((int)count/2-1) + al.get((int)count/2))/2.0f);
        }
        else
        {
            msdw.setMedian_Ratings(al.get((int)count/2));
        }

        float avg = sum/count;
        float sumOfSquares =0.0f;
        for(float res :al){
            sumOfSquares+= Math.pow(res-avg,2);
        }
        msdw.setStandardDeviation_Ratings((float)Math.sqrt(sumOfSquares/(count-1)));
        context.write(key,msdw);
    }
}
