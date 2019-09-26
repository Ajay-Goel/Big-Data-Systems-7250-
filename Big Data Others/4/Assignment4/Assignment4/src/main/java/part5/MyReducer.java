package part5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<IntWritable,AverageWritable,IntWritable,AverageWritable> {

    private AverageWritable aw = new AverageWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<AverageWritable> values, Context context) throws IOException, InterruptedException {
        double sum =0;
        long count =0;

        for(AverageWritable spaw: values){
            System.out.println("average : "+spaw.getAverage()+"count :"+spaw.getCount());
                sum += spaw.getAverage() * spaw.getCount();

                count+= spaw.getCount();
            System.out.println(sum+"---"+count);
            //System.out.println(spaw.getAverage()+"---"+spaw.getCount());
        }
        aw.setCount(count);
        aw.setAverage(sum/count);
        context.write(key,aw);

    }
}
