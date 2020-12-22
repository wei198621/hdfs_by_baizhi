package com.tiza.leo.mapreduce.cutmulti;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * @author leowei
 * @date 2020/12/23  - 0:24
 */
public class CutMultiJob extends Configured implements Tool {
    private Logger logger = Logger.getLogger(CutMultiJob.class);

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CutMultiJob(),args);
    }

    public int run(String[] strings) throws Exception {
        logger.info("============ CutMultiJob  start run...... ==============");
        // 1  创建job
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(CutMultiJob.class);

        // 2  设置inputformat
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/cutmulti/"));

        // 3  设置map阶段
        job.setMapperClass(CutMultiMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 4  设置shuffle 阶段  (默认无需配置)

        // 5  设置reduce 阶段
        job.setReducerClass(CutMultiReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 6  设置 output Formate   注意:要求结果目录不能存在
        job.setOutputFormatClass(TextOutputFormat.class);
        Path resPath = new Path("/cutmultiResult/result");  //此为目录 会在 此目录下 生成/mobileaccesslog/result/part-r-00000 名的结果文件
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(resPath)){
            fileSystem.delete(resPath,true);
        }
        TextOutputFormat.setOutputPath(job,resPath);

        // 7  提交job作业
        boolean result = job.waitForCompletion(true);
        System.out.println("CutMultiJob result = " + result);
        logger.info("============ CutMultiJob  end  ...   CutMultiJob result = " + result);
        return 0;
    }
}
