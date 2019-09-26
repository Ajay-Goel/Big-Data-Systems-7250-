package part5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text, IntWritable, AverageWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String [] Tokens = value.toString().split(",");
        //AverageWritable spaw = new AverageWritable();
        //1,7,8
        if (value.toString().contains("stock_price_high")) {
            return;
        } else {
            String [] i = Tokens[2].split("-");
            int year = Integer.parseInt(i[0]);
            AverageWritable spaw = new AverageWritable(Double.parseDouble(Tokens[8]),1);
          //  spaw.setAverage(Float.parseFloat(Tokens[8]));
            //spaw.setCount(1);
            //System.out.println(spaw.getAverage()+"---"+spaw.getCount());
            //System.out.println(spaw.getCount());
            System.out.println("Mapper Year:"+ new IntWritable(year)+" Mapper Average:" + spaw.getAverage());
            context.write(new IntWritable(year),spaw);
            ;
        }
    }
}
