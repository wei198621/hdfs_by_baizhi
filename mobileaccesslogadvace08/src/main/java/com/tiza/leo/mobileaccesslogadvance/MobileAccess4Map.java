package com.tiza.leo.mobileaccesslogadvance;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author: tz_wl
 * Date: 2020/12/23 17:16
 1363157985066   13726230503     00-FD-07-A4-72-B8:CMCC  120.196.100.82  24      27      2481    24681   200
 1363157995052   13826544101     5C-0E-8B-C7-F1-E0:CMCC  120.197.40.4    4       0       264     0       200
 */
public class MobileAccess4Map extends Mapper<LongWritable,Text,Text,AccessLog4Writable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //计数器
        Counter access_map = context.getCounter("map-group", "access-map");
        access_map.increment(1);

        String[] values = value.toString().split("\t");
        int upload = Integer.valueOf(values[values.length-3]);
        int down = Integer.valueOf(values[values.length-2]);
        int total = Integer.valueOf(values[values.length-1]);
        context.write(new Text(values[1]),new AccessLog4Writable(upload,down,total));
    }
}
