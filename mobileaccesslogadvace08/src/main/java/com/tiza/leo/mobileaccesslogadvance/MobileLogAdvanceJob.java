package com.tiza.leo.mobileaccesslogadvance;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Author: tz_wl
 * Date: 2020/12/24 14:22
 * Content:
 */
public class MobileLogAdvanceJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MobileLogAdvanceJob(),args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        //01 创建job作业
        Job job = Job.getInstance(getConf(), "customer-type-job");
        job.setJarByClass(MobileLogAdvanceJob.class);

        //02 设置input format
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/mobileaccesslog/data"));

        //03 设置map
        job.setMapperClass(MobileAccess4Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AccessLog4Writable.class);

        //04 shuffle 无须设置 自动处理
        ////设置Combiner
        job.setCombinerClass(MobileAccess4Reduce.class);

        //05 设置reduce
        job.setReducerClass(MobileAccess4Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AccessLog4Writable.class);
        //默认reduce的数量是1  默认使用的是HashPartitioner会根据map输出key做hash运算去决定map中输出数据交给那个reduce处理
        //job.setNumReduceTasks(3); //设置reduce 为 3 个   //HashPartitioner
        // --------------------
        //job.setNumReduceTasks(2);//如果自定义分组4个，设置个数为2 ，会报如下错误  Error: java.io.IOException: Illegal partition for 13726230503 (2)
        //job.setPartitionerClass(ProvincePartitioner.class);
        // --------------------
        // 如果设置
        job.setNumReduceTasks(4);
        job.setPartitionerClass(ProvincePartitioner.class);


        //06 设置Output Format
        job.setOutputFormatClass(TextOutputFormat.class);
        Path res = new Path("/mobileaccesslog/result_plus_advance");
        FileSystem fileSystem = FileSystem.get(getConf());
        if(fileSystem.exists(res)){
            fileSystem.delete(res,true);
        }
        TextOutputFormat.setOutputPath(job, res);

        //07 提交作业
        boolean status = job.waitForCompletion(true);
        System.out.println("作业执行状态:" + status);



        return 0;
    }
}
