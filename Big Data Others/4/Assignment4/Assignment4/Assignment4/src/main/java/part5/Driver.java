package part5;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException{
//        if (args.length != 2) {
//            System.err.println("Usage: MaxSubmittedCharge <input path> <output path>");
//            System.exit(-1);
//        }

        Job job = Job.getInstance();
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(AverageWritable.class);
        //job.setCombinerClass(MyReducer.class);
        job.setNumReduceTasks(1);






        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        FileInputFormat.addInputPath(job, inputPath);

        FileOutputFormat.setOutputPath(job, outputDir);





        //Configuration conf = new Configuration(true);

        //job.setJarByClass(Driver.class);







//        job.setMapOutputKeyClass(IntWritable.class);
//        job.setMapOutputValueClass(AverageWritable.class);






        // Delete output if exists
//        FileSystem hdfs = FileSystem.get(job.getConfiguration());
//        if (hdfs.exists(outputDir)) {
//            hdfs.delete(outputDir, true);
//        }

        int code = job.waitForCompletion(true) ? 0 : 1;
        if(code==0){
            System.out.println("DOne");
            System.exit(code);
        }
        else
        {
            System.exit(code);
        }

    }
}
