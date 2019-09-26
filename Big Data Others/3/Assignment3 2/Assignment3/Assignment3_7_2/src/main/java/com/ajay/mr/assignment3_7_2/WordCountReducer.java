package com.ajay.mr.assignment3_7_2;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author ajaygoel
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        IntWritable result= new IntWritable();
        for(IntWritable v: values){
            sum += v.get();
        }
        result.set(sum);
        context.write(key, result);
        
        
    }
    
    
}
