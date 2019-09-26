/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ajay.mr.assignment3_7_1;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author ajaygoel
 */
public class CombineFileMR {
    
    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        // Create a new Job
        Job job = Job.getInstance(conf,"combineFile");
        job.setJarByClass(CombineFileMR.class);
        conf.set("mapred.max.split.size", "134217728");//128 MB
        
        String[] jobArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

        // Specify various job-specific parameters     
        job.setJobName("myjob");
             
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setInputFormatClass(CombineFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
       // ExtendedCombineFileInputFormat.addInputPath(conf, new Path(jobArgs[0]));
        
        
        job.setMapperClass(CombineFileMapper.class);
        job.setReducerClass(CombineFileReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        System.exit(job.waitForCompletion(true)?0:1);
        // Submit the job, then poll for progress until the job is complete
        
    }
    
}
