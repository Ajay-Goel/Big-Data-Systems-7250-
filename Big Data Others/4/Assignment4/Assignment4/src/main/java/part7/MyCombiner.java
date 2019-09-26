package part7;


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;

public class MyCombiner  extends Reducer<IntWritable, SortedMapWritable,IntWritable,SortedMapWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
        SortedMapWritable smw = new SortedMapWritable();
        for(SortedMapWritable res : values) {
            for (Object e : res.entrySet()) {
                Map.Entry<WritableComparable, Writable> entry =  (Map.Entry<WritableComparable, Writable>) e;
                LongWritable count = (LongWritable) smw.get(entry.getKey());
                if(count!=null){
                    count.set(count.get()+ ((LongWritable)entry.getValue()).get());
                }
                else{
                    smw.put(entry.getKey(),new LongWritable(((LongWritable)entry.getValue()).get()));
                }
            }
            res.clear();
        }
        context.write(key,smw);
    }
}
