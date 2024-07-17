package com.opstty.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text district = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(";");

        // 忽略第一行标题行
        if (key.get() == 0 && line.contains("GEOPOINT")) {
            return;
        }

        if (fields.length > 1) {
            district.set(fields[1]); // 第2列为区
            context.write(district, one);
        }
    }
}
