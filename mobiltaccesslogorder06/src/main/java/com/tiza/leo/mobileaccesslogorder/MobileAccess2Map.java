package com.tiza.leo.mobileaccesslogorder;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author: tz_wl
 * Date: 2020/12/23 17:16
 * Content:
 *
 *   map 的输入为 如下 也就是Text 为如下
 13726230503     4962    49362   54324
 13826544101     528     0       528
 13926251106     480     0       480
 13926435656     264     3024    3288
 *
 *
 */
public class MobileAccess2Map extends Mapper<LongWritable, Text,AccessLog2Writable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        int upload = Integer.valueOf(values[values.length-3]);
        int down = Integer.valueOf(values[values.length-2]);
        int total = Integer.valueOf(values[values.length-1]);
        context.write(new AccessLog2Writable(upload,down,total),new Text(values[1]));
    }
}
