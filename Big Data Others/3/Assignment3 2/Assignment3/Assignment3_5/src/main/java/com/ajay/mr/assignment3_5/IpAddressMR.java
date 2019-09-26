/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ajay.mr.assignment3_5;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



/**
 *
 * @author ajaygoel
 */
public class IpAddressMR {

    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        // Create a new Job
        Job job = Job.getInstance(conf,"ipAddressCount");
        job.setJarByClass(IpAddressMR.class);

        // Specify various job-specific parameters     
        job.setJobName("myIPJob");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(FiveMapper.class);
        job.setReducerClass(FiveReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        // Submit the job, then poll for progress until the job is complete
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
