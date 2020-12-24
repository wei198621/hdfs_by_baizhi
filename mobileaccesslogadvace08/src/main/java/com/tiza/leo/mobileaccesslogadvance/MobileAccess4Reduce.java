package com.tiza.leo.mobileaccesslogadvance;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author: tz_wl
 * Date: 2020/12/23 17:16
 key:1363157985066
 value:[AccessLog4Writable,AccessLog4Writable]
 */
public class MobileAccess4Reduce extends Reducer<Text,AccessLog4Writable,Text, AccessLog4Writable> {

   private static final int TOPN  = Integer.MAX_VALUE;     // 实现topn 只需在 reduce方法加上 四句话    此为01
   int sum=0;                                  //此为02
    @Override
    protected void reduce( Text key, Iterable<AccessLog4Writable> values, Context context) throws IOException, InterruptedException {
        if (sum < TOPN) {                    //此为03
            int upload=0;
            int down=0;
            int total=0;
            for (AccessLog4Writable accessLog : values) {
                upload+= accessLog.getUpload();
                down+=accessLog.getDown();
                total+=accessLog.getTotal();
            }
            context.write(key, new AccessLog4Writable(upload,down,total));
            sum++;                           //此为01
        }
    }
}
