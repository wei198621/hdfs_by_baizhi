package com.tiza.leo.mapreduce.cutmulti;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author leowei
 * @date 2020/12/23  - 0:25
 */
public class CutMultiMap extends Mapper<LongWritable,Text,Text,NullWritable> {

    private Logger logger = Logger.getLogger(CutMultiMap.class);

    /**
     *
     * @param key      没有使用 （原始数据的字符偏移量，作为key ）
     * @param value
    2017-12-09 a
    2017-12-10 a
    2017-12-11 a
    2017-12-12 b
    2017-12-13 b


     * @param context
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        logger.info("  current key:" + key + "  curent value: " + value + ";");
        context.write(value, NullWritable.get());
        //super.map(key, value, context);
    }
}
