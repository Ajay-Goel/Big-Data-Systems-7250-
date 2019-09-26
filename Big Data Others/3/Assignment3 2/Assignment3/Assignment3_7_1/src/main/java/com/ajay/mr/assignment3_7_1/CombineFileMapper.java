/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ajay.mr.assignment3_7_1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author ajaygoel
 */
public class CombineFileMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    Text txtKey = new Text();
    Text txtValue = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String a = value.toString();
        String[] b = a.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        
        //if (!(b[0].equals("Name")))
            //context.write(new Text(b[0]), new Text(b[2]));
            context.write(new Text(b[0]), new Text("hi"));
            //context.write(key, value);
    }
}
