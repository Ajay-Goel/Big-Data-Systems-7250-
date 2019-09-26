package part5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class StockReducer extends Reducer<Text, StockWritable, Text, StockWritable>{
    double sum = 0.0;
    long count = 0;
    @Override
    public void reduce(Text key, Iterable<StockWritable> values, Context ctx) throws IOException, InterruptedException{
        for(StockWritable val: values){
            sum+=val.getAvg()*val.getCount();
            count+=val.getCount();
        }
        StockWritable result = new StockWritable(sum/count, count);
        ctx.write(key, result);
    }
}

