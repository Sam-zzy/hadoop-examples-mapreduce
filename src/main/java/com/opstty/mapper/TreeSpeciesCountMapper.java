package com.opstty.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeSpeciesCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text species = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(";");

        // 忽略第一行标题行
        if (key.get() == 0 && line.contains("ID")) {
            return;
        }

        // 提取树种字段（假设树种在第3列，索引为2）
        if (fields.length > 2) {
            species.set(fields[2]);
            context.write(species, one);
        }
    }
}
