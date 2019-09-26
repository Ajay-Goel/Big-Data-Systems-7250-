package part4;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ajaygoel
 */
public class MyMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String [] Tokens = value.toString().split(",");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");


        if (value.toString().contains("stock_price_high")) {
            return;
        } else {
            try {
                CompositeKeyWritable out = new CompositeKeyWritable(Tokens[1],sdf.parse(Tokens[2]));
                context.write(out, NullWritable.get());
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }
    }
}
