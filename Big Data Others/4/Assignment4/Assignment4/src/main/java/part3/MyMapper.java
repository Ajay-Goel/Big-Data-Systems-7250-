package part3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MyMapper extends Mapper<LongWritable, Text,Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //max stock_volume,  min stock_volume, max stock_price_adj_close
        // key - stock symbol and rest will belong to the text object in the reducer
        String [] Tokens = value.toString().split(",");
        Text text = new Text(Tokens[7]+","+Tokens[7]+","+Tokens[8]);
        //1,7,8
        if (value.toString().contains("stock_price_high")) {
            return;
        } else {
            context.write(new Text(Tokens[1]),text);
        }
    }
}
