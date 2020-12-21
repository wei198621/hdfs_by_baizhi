package com.tiza.leo.mapreduct.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Author: tz_wl
 * Date: 2020/12/21 15:01
 * Content:
 */
public class WordCountJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new WordCountJob(),args);
    }

    /*
// 1  创建job                    //设置数据输入路径
// 2  设置inputformat            原始数据  zhangsan lisi wangwu     --第一行
										   zhangsan                  --第二行
// 3  设置map阶段                           zhangsan 1
                                           lisi  1
                                           wangwu 1
                                           zhangsan 1

// 4  设置shuffle 阶段  (默认无需配置)       zhangsan [1,1]
                                          lisi  1
                                          wangwu 1
// 5  设置reduce 阶段                      zhangsan 2
                                          lisi  1
                                          wangwu 1
// 6  设置 output Formate    // 设置数据输出路径    注意:要求结果目录不能存在
// 7  提交job作业
     */

    public int run(String[] strings) throws Exception {
        // 1  创建job
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountJob.class);

        // 2  设置inputformat
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/wordcount/data"));

        // 3  设置map阶段
        job.setMapperClass(WordCountMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 4  设置shuffle 阶段  (默认无需配置)

        // 5  设置reduce 阶段
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 6  设置 output Formate   注意:要求结果目录不能存在
        job.setOutputFormatClass(TextOutputFormat.class);
        Path resPath = new Path("/wordcount/result");  //此为目录 会在 此目录下 生成/wordcount/result/part-r-00000 名的结果文件
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(resPath)){
            fileSystem.delete(resPath,true);
        }
        TextOutputFormat.setOutputPath(job,resPath);

        // 7  提交job作业
        boolean result = job.waitForCompletion(true);
        System.out.println("wordcount result = " + result);
        return 0;
    }
}
