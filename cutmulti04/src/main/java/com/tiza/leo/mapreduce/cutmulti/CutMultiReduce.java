package com.tiza.leo.mapreduce.cutmulti;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author leowei
 * @date 2020/12/23  - 0:25
 */
public class CutMultiReduce extends Reducer<Text,NullWritable,Text,NullWritable> {
    private Logger logger=Logger.getLogger(CutMultiReduce.class);
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        logger.info("====reduce key:"+ key + " ;  value: "+ values.toString()+ "============");
        context.write(key,NullWritable.get() );
    }
}
