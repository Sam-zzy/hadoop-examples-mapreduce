package com.opstty.mapper;

import com.opstty.TreeAgeWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<LongWritable, Text, IntWritable, TreeAgeWritable> {
    private IntWritable age = new IntWritable();
    private TreeAgeWritable treeAgeWritable = new TreeAgeWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(";");

        // 忽略第一行标题行
        if (key.get() == 0 && line.contains("GEOPOINT")) {
            System.out.println("Skipping header line: " + line);
            return;
        }

        // 检查数据格式
        System.out.println("Processing line: " + line);
        if (fields.length > 5) {
            try {
                int currentYear = 2023; // 当前年份，根据需要更新
                int plantingYear = Integer.parseInt(fields[5]);
                int treeAge = currentYear - plantingYear;
                age.set(treeAge);
                treeAgeWritable = new TreeAgeWritable(age.get(), fields[1]);
                context.write(new IntWritable(1), treeAgeWritable); // 使用常量键将所有数据发送到一个 Reducer 实例
                System.out.println("Map output: " + age.get() + " - " + fields[1]);
            } catch (NumberFormatException e) {
                System.out.println("Skipping line due to NumberFormatException: " + line);
                // 忽略解析错误的行
            }
        } else {
            System.out.println("Skipping line due to insufficient fields: " + line);
        }
    }
}
