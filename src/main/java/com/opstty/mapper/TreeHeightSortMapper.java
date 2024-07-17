package com.opstty.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeHeightSortMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
    private FloatWritable height = new FloatWritable();
    private Text species = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(";");

        // 忽略第一行标题行
        if (key.get() == 0 && line.contains("ID")) {
            return;
        }

        // 提取高度字段和树种字段（假设树种在第3列，索引为2，高度在第6列，索引为5）
        if (fields.length > 5) {
            try {
                height.set(Float.parseFloat(fields[5]));
                species.set(fields[2]);
                context.write(height, species);
            } catch (NumberFormatException e) {
                // 忽略解析错误的行
            }
        }
    }
}
