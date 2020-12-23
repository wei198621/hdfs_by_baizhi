package com.tiza.leo.mapreduce.mobileaccesslogplus;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Author: tz_wl
 * Date: 2020/12/23 17:16
 * Content:
 */
public class MobileAccessPlusJob  extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MobileAccessPlusJob(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance(getConf(), "customer-type-job");
        job.setJarByClass(MobileAccessPlusJob.class);

        //设置input format
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/mobileaccesslog/data"));

        //设置map
        job.setMapperClass(MobileAccessPlusMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AccessLogWritable.class);

        //shuffle 无须设置 自动处理

        //设置reduce
        job.setReducerClass(MobileAccessPlusReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AccessLogWritable.class);

        //设置Output Format
        job.setOutputFormatClass(TextOutputFormat.class);
        Path res = new Path("/mobileaccesslog/result_plus");  //此路径需要手工创建
        FileSystem fileSystem = FileSystem.get(getConf());
        if(fileSystem.exists(res)){
            fileSystem.delete(res,true);
        }
        TextOutputFormat.setOutputPath(job, res);

        //提交作业
        boolean status = job.waitForCompletion(true);
        System.out.println("作业执行状态:" + status);

        return 0;
    }
}
