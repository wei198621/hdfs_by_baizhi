package com.tiza.leo.mapreduce.mobileaccesslog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author: tz_wl
 * Date: 2020/12/22 17:12
 * Content:
 */
public class MobileAccessMap extends Mapper<LongWritable,Text,Text,Text> {
    /**
     *
     * @param key      没有使用 （原始数据的字符偏移量，作为key ）
     * @param value    读入的一行数据              倒数第三上行   倒数第二下行
     *                 1363157985066	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	24	27	2481	24681	200
     *                 0                 1               2                    3             4   5    6        7     8
     *                             第2列手机号作为key                                                上行      下行
     * @param context  上下文
     *                 输出
     *                   key                      value
     *                 13726230503              2481,24681
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("current key:" + key + "curent value: " + value + ";");
        String[] values = value.toString().split("\t");  //不是空格
        int length = values.length;  //长度  共8 如果小于8 报错
        String keyOut = "";
        String ValueOut = "";
        if (length < 8) {
        } else {
            keyOut = values[1];
            ValueOut = values[6] + "," + values[7];
        }
        context.write(new Text(keyOut), new Text(ValueOut));
        //super.map(key, value, context);
    }
}
