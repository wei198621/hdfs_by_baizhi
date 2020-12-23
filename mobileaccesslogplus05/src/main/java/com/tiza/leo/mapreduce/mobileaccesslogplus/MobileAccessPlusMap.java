package com.tiza.leo.mapreduce.mobileaccesslogplus;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author: tz_wl
 * Date: 2020/12/23 17:16
 * Content:
 */
public class MobileAccessPlusMap extends Mapper<LongWritable, Text,Text, AccessLogWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        int upload = Integer.valueOf(values[values.length-3]);
        int down = Integer.valueOf(values[values.length-2]);
        context.write(new Text(values[1]),new AccessLogWritable(upload,down,0));
    }
}
