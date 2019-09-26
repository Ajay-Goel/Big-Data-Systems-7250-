/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ajay.mr.assignment3_6;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ajaygoel
 */
public class NYSEMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    FloatWritable price = new FloatWritable();
    Text stock = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String cvsSplitBy = ",";
        int iteration = 0;
        //if(iteration==0)
//          /  iteration++;
        if (value.toString().contains("stock_price_high")) {
            return;
        } else {
            String[] data = line.split(cvsSplitBy);
            stock.set(data[1]);
            price.set(Float.parseFloat(data[4]));

            context.write(stock, price);
        }

    }

}
