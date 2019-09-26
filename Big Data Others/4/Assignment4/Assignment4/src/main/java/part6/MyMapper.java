package part6;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text,Text,RatingWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String Tokens[] = value.toString().split("::");

        Text MovieId = new Text(Tokens[1]);
        RatingWritable rw = new RatingWritable(Integer.parseInt(Tokens[2]),1);
        context.write(MovieId,rw);
    }
}
