package com.tiza.leo.mapreduce.mobileaccesslogplus;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author: tz_wl
 * Date: 2020/12/23 17:16
 * Content:
 */
public class MobileAccessPlusReduce extends Reducer<Text, AccessLogWritable,Text, AccessLogWritable> {
    @Override
    protected void reduce(Text key, Iterable<AccessLogWritable> values, Context context) throws IOException, InterruptedException {
        int upload =0;
        int down = 0;
        for (AccessLogWritable value : values) {
            upload += value.getUpload();
            down  += value.getDown();
        }
        context.write(key,new AccessLogWritable(upload,down,upload+down));
    }
}
