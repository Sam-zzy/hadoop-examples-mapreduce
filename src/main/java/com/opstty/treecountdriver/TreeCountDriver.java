package com.opstty.treecountdriver;

import com.opstty.mapper.TreeCountMapper;
import com.opstty.reducer.TreeCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TreeCountDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: TreeCountDriver <program name> <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Tree Count by District");
        job.setJarByClass(TreeCountDriver.class);
        job.setMapperClass(TreeCountMapper.class);
        job.setReducerClass(TreeCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TreeCountDriver(), args);
        System.exit(exitCode);
    }
}
