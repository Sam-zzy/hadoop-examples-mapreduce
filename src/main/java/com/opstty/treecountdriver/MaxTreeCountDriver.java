package com.opstty.treecountdriver;

import com.opstty.mapper.MaxTreeCountMapper;
import com.opstty.reducer.MaxTreeCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTreeCountDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MaxTreeCountDriver <program name> <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Max Tree Count District");
        job.setJarByClass(MaxTreeCountDriver.class);
        job.setMapperClass(MaxTreeCountMapper.class);
        job.setReducerClass(MaxTreeCountReducer.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTreeCountDriver(), args);
        System.exit(exitCode);
    }
}
