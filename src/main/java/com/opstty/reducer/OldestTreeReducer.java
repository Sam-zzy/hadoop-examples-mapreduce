package com.opstty.reducer;

import com.opstty.TreeAgeWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<IntWritable, TreeAgeWritable, Text, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<TreeAgeWritable> values, Context context) throws IOException, InterruptedException {
        int oldestAge = Integer.MIN_VALUE;
        String district = null;

        for (TreeAgeWritable val : values) {
            if (val.getAge().get() > oldestAge) {
                oldestAge = val.getAge().get();
                district = val.getDistrict().toString();
            }
        }

        if (district != null) {
            context.write(new Text(district), new IntWritable(oldestAge));
            System.out.println("Reduce output: " + district + " - " + oldestAge); // 调试信息
        }
    }
}
