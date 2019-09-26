package part5;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class StockMapper extends Mapper<LongWritable,Text, Text, StockWritable> {

    private IntWritable count = new IntWritable();


    public void map(LongWritable key,Text value, Context ctx) throws IOException, InterruptedException{
        try {
            String[] tokens = value.toString().split(",");
            //Text stockKey = new Text(tokens[1] + " " + tokens[2].substring(0, 4)) ;
            Text stockKey = new Text(tokens[2].substring(0, 4)) ;
            StockWritable stockValue = new StockWritable(Double.parseDouble(tokens[8]), 1);
            ctx.write(stockKey, stockValue);
        }catch(NumberFormatException ne) {
            System.out.println("Headers");
        }
    }
}

