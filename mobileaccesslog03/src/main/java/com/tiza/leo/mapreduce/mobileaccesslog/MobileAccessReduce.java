package com.tiza.leo.mapreduce.mobileaccesslog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author: tz_wl
 * Date: 2020/12/22 17:12
 * Content:
 */
public class MobileAccessReduce extends Reducer<Text,Text,Text,Text> {
    /**
     *  map的输出作为 reduce 的输入
     *                   key                      value
     *                 13726230501              281,281
     *                 13726230503              2481,24681 111,222 333,444
     *
     * @param key            13726230503
     * @param values         2481,24681  或者   2481,24681 111,222 333,444
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long upValue=0L;
        long downValue=0L;
        long allValue=0L;
        for (Text value : values) {
            System.out.println("current key:" + key + "curent value: "+ value+ ";");
            String[] mobileMes = value.toString().split(",");
            upValue = Long.parseLong(mobileMes[0]);
            downValue =Long.parseLong(mobileMes[1]);
            allValue = upValue+downValue;
        }
        context.write(key,new Text("up:"+upValue+",down:"+downValue+",all:"+allValue));
        //super.reduce(key, values, context);
    }
}
