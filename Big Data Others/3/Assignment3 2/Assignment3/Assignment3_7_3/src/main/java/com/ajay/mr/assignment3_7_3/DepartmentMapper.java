/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ajay.mr.assignment3_7_3;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ajaygoel
 */
public class DepartmentMapper extends Mapper<Text, Text, Text, IntWritable> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String tokens[] = line.split("\t");
        Text a = value;
        
        if(!a.toString().equals("Name")){
            context.write(a,new IntWritable(1));
        }
    }
    
}
