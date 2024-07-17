package com.opstty.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeMaxHeightMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private Text species = new Text();
    private FloatWritable height = new FloatWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(";");

        // 忽略第一行标题行
        if (key.get() == 0 && line.contains("ID")) {
            return;
        }

        // 提取树种字段和高度字段（假设树种在第3列，索引为2，高度在第6列，索引为5）
        if (fields.length > 5) {
            species.set(fields[2]);
            try {
                height.set(Float.parseFloat(fields[5]));
                context.write(species, height);
            } catch (NumberFormatException e) {
                // 忽略解析错误的行
            }
        }
    }
}
