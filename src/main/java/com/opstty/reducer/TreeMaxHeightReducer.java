package com.opstty.reducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TreeMaxHeightReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float maxHeight = Float.MIN_VALUE;
        for (FloatWritable val : values) {
            if (val.get() > maxHeight) {
                maxHeight = val.get();
            }
        }
        context.write(key, new FloatWritable(maxHeight));
    }
}
