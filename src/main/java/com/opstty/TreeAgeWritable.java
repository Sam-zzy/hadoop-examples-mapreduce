package com.opstty;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TreeAgeWritable implements Writable {
    private IntWritable age;
    private Text district;

    public TreeAgeWritable() {
        this.age = new IntWritable();
        this.district = new Text();
    }

    public TreeAgeWritable(int age, String district) {
        this.age = new IntWritable(age);
        this.district = new Text(district);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        age.write(out);
        district.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        age.readFields(in);
        district.readFields(in);
    }

    public IntWritable getAge() {
        return age;
    }

    public Text getDistrict() {
        return district;
    }

    @Override
    public String toString() {
        return age.toString() + "\t" + district.toString();
    }
}
