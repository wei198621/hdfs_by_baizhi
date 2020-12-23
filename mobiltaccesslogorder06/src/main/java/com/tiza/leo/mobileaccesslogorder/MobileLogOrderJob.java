package com.tiza.leo.mobileaccesslogorder;

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
 * @author leowei
 * @date 2020/12/23  - 23:24
 */
public class MobileLogOrderJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MobileLogOrderJob(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance(getConf(), "customer-type-job");
        job.setJarByClass(MobileLogOrderJob.class);

        //设置input format
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/mobileaccesslog/result_plus/part-r-00000")); //第五个程序的友好结果作为此程序的输入

        //设置map
        job.setMapperClass(MobileAccess2Map.class);
        job.setMapOutputKeyClass(AccessLog2Writable.class);
        job.setMapOutputValueClass(Text.class);

        //shuffle 无须设置 自动处理

        //设置reduce
        job.setReducerClass(MobileAccess2Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AccessLog2Writable.class);

        //设置Output Format
        job.setOutputFormatClass(TextOutputFormat.class);
        Path res = new Path("/mobileaccesslog/result_plus_order");
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
