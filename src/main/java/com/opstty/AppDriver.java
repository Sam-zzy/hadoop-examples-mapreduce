package com.opstty;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AppDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: AppDriver <driver class> <input path> <output path>");
            return -1;
        }

        String driverClassName = args[0];
        String inputPath = args[1];
        String outputPath = args[2];

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, driverClassName);
        job.setJarByClass(Class.forName(driverClassName));

        // 根据类名设置 Mapper 和 Reducer
        if (driverClassName.equals("com.opstty.treecountdriver.TreeCountDriver")) {
            job.setMapperClass(com.opstty.mapper.TreeCountMapper.class);
            job.setReducerClass(com.opstty.reducer.TreeCountReducer.class);
            job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
            job.setOutputValueClass(org.apache.hadoop.io.IntWritable.class);
        } else if (driverClassName.equals("com.opstty.treecountdriver.MaxTreeCountDriver")) {
            job.setMapperClass(com.opstty.mapper.MaxTreeCountMapper.class);
            job.setReducerClass(com.opstty.reducer.MaxTreeCountReducer.class);
            job.setMapOutputKeyClass(org.apache.hadoop.io.NullWritable.class);
            job.setMapOutputValueClass(org.apache.hadoop.io.Text.class);
            job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
            job.setOutputValueClass(org.apache.hadoop.io.IntWritable.class);
        } else {
            System.err.println("Unknown driver class: " + driverClassName);
            return -1;
        }

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new AppDriver(), args);
        System.exit(exitCode);
    }
}
