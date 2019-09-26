package part4;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
        //Configuration conf = new Configuration();
        // Create a new Job
        // Job job = Job.getInstance(conf,"lab5");
        Job job = Job.getInstance();
        job.setJarByClass(Driver.class);

        job.setGroupingComparatorClass(GroupComparator.class);
        job.setSortComparatorClass(SecondarySortComparator.class);
        job.setPartitionerClass(KeyPartition.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outDir = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outDir);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(1);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(CompositeKeyWritable.class);
        job.setOutputValueClass(NullWritable.class);

        FileSystem fs = FileSystem.get(job.getConfiguration());
        if (fs.exists(outDir)) {
            fs.delete(outDir, true);
        }
        job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true)?0:1);
        // Submit the job, then poll for progress until the job is complete
    }
}
