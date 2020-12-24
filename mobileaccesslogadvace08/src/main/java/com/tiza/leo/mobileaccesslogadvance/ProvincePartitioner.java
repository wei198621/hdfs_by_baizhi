package com.tiza.leo.mobileaccesslogadvance;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * Author: tz_wl
 * Date: 2020/12/24 16:03
 * Content:
 */
public class ProvincePartitioner extends Partitioner<Text,AccessLog4Writable> {

    private static HashMap<String,Integer> provincePartitioners = new HashMap<>();
    static {   //按照手机号码前3位区分  共4个分区
        provincePartitioners.put("130",0);
        provincePartitioners.put("131",0);
        provincePartitioners.put("132",0);
        provincePartitioners.put("133",1);
        provincePartitioners.put("134",1);
        provincePartitioners.put("135",1);
        provincePartitioners.put("136",2);
        provincePartitioners.put("137",2);
        provincePartitioners.put("138",2);
        provincePartitioners.put("139",3);
    }

    //  按照hashmap中的值分配reduceId
    @Override
    public int getPartition(Text key, AccessLog4Writable accessLog4Writable, int numPartitions) {
        String keyPrefix = key.toString().substring(0, 3);
        Integer partionId = provincePartitioners.get(keyPrefix);
        return partionId==null ? 3 :partionId;
    }
}
