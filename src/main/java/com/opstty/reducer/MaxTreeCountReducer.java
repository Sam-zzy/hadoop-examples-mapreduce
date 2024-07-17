package com.opstty.reducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTreeCountReducer extends Reducer<NullWritable, Text, Text, IntWritable> {
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int maxCount = Integer.MIN_VALUE;
        String districtWithMaxTrees = null;

        for (Text value : values) {
            String[] fields = value.toString().split("\t");
            String district = fields[0];
            int count = Integer.parseInt(fields[1]);

            if (count > maxCount) {
                maxCount = count;
                districtWithMaxTrees = district;
            }
        }

        if (districtWithMaxTrees != null) {
            context.write(new Text(districtWithMaxTrees), new IntWritable(maxCount));
        }
    }
}
