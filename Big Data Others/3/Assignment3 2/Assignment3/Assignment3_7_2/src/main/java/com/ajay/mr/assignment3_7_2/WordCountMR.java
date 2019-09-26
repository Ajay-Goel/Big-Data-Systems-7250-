package com.ajay.mr.assignment3_7_2;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author ajaygoel
 */
public class WordCountMR {

    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        // Create a new Job
        
        // be sure to set the length of your fixed length records, so the
// FixedLengthRecordReader can extract the records correctly.
    conf.setInt(FixedLengthInputFormat.FIXED_RECORD_LENGTH, 502);
 
// OR alternatively you can set it this way, the name of the property is
// "mapreduce.input.fixedlengthinputformat.record.length"
//myJobConf.setInt("mapreduce.input.fixedlengthinputformat.record.length",502);

        Job job = Job.getInstance(conf,"wordcount");
        job.setJarByClass(WordCountMR.class);

        // Specify various job-specific parameters     
        job.setJobName("myjob");
        
        
   
        
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setInputFormatClass(FixedLengthInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        System.exit(job.waitForCompletion(true)?0:1);
        // Submit the job, then poll for progress until the job is complete
        
    }

}
