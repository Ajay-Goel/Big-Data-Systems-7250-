package com.ajay.mr.assignment3_7_2;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author ajaygoel
 */
public class WordMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    
    //value needs to be converted to hadoop datatypehadoop datatype
    IntWritable one = new IntWritable(1);
    //Key
    Text word = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        String line = value.toString();
        String[] tokens = line.split(" ");
        
        for(String s:tokens){
            word.set(s);
            context.write(word, one);
        }
            
        
    }

    
    
}
