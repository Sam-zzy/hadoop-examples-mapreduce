package com.opstty.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeDistrictMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // 忽略标题行
        if (key.get() == 0 && line.contains("ARRONDISSEMENT")) {
            return;
        }
        String[] fields = line.split(";");
        String district = fields[1]; // ARRONDISSEMENT 列的索引是 1
        context.write(new Text(district), NullWritable.get());
    }
}
