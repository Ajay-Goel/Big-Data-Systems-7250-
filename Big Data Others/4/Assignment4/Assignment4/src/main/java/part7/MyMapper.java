package part7;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<Object, Text, IntWritable, SortedMapWritable> {
    private IntWritable outYear = new IntWritable();
    private static final LongWritable one = new LongWritable(1);
    private DoubleWritable avg = new DoubleWritable();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String Tokens[] = value.toString().split(",");
        if (value.toString().contains("stock_price_high")) {
            return;
        } else {
            double stockAdjClose = Double.parseDouble(Tokens[8]);
            String year []= Tokens[2].split("-");
            int yr = Integer.parseInt(year[0]);
            outYear.set(yr);
            avg.set(stockAdjClose);
            SortedMapWritable outAvgPrice = new SortedMapWritable();
            outAvgPrice.put(avg,one);
            context.write(outYear,outAvgPrice);
        }
    }
}
