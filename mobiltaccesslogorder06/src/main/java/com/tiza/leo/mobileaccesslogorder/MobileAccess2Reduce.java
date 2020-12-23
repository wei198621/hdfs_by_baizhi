package com.tiza.leo.mobileaccesslogorder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author: tz_wl
 * Date: 2020/12/23 17:16
 * Content:
 */
public class MobileAccess2Reduce extends Reducer<AccessLog2Writable,Text,Text, AccessLog2Writable> {

   private static final int TOPN  = 3;     // 实现topn 只需在 reduce方法加上 四句话    此为01
   int sum=0;                                  //此为02
    @Override
    protected void reduce(AccessLog2Writable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
       if(sum<TOPN) {                  //此为03
           for (Text value : values) {
               context.write(value, key);
           }
           sum ++;             //此为01
       }
    }
}
