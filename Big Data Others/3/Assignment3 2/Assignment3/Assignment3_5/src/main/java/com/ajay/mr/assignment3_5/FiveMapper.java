        /*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ajay.mr.assignment3_5;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ajaygoel
 */
public class FiveMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    
    IntWritable one = new IntWritable(1);
    Text ip = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String [] tokens = line.split(" - - ");
        ip.set(tokens[0]);
        context.write(ip, one);
    }
    
    
}
