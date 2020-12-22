package com.tiza.leo.mapreduct.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author: tz_wl
 * Date: 2020/12/21 15:54
 * Content:
 *
 *
 *  // 1  创建job                    //设置数据输入路径
 // 2  设置inputformat            原始数据   zhangsan lisi wangwu  --第一行
                                           zhangsan              --第二行
 // 3  设置map阶段                           zhangsan 1
                                            lisi  1
                                            wangwu 1
                                            zhangsan 1
 // 4  设置shuffle 阶段  (默认无需配置)        zhangsan [1,1]
                                            lisi  1
                                            wangwu 1
 // 5  设置reduce 阶段                       zhangsan 2
                                            lisi  1
                                            wangwu 1
 // 6  设置 output Formate    // 设置数据输出路径    注意:要求结果目录不能存在
 // 7  提交job作业

 *
 */
public class WordCountMap extends Mapper<LongWritable,Text,Text,LongWritable> {

    /**
     *
     * @param key      没有使用 （原始数据的字符偏移量，作为key ）
     * @param value    读入的一行数据
     * @param context  上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("current key:" + key + "curent value: "+ value+ ";");
        String[] keys = value.toString().split(" ");
        for (String word : keys) {
            context.write(new Text(word),new LongWritable(1));
        }
        //super.map(key, value, context);
    }
}
