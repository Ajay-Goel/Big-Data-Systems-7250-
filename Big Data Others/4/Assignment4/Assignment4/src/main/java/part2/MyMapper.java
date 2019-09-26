package part2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MyMapper extends Mapper<LongWritable, Text,Text,StockWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String [] Tokens = value.toString().split(",");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");


        if (value.toString().contains("stock_price_high")) {
            return;
        } else {
            try {
                StockWritable sw = new StockWritable(sdf.parse(Tokens[2]),sdf.parse(Tokens[2]),Long.parseLong(Tokens[7]),Float.parseFloat(Tokens[8]));
                //System.out.println(sw);
                context.write(new Text(Tokens[1]),sw);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

    }
}
