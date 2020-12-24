package com.tiza.leo.mobileaccesslogmap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/**
 * Author: tz_wl
 * Date: 2020/12/24 10:00
 * Content:
 */
public class MobileLogMapJob extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MobileLogMapJob(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {

        //01 创建job
        Job job = Job.getInstance(getConf(),"mobileAccess07OnlyMapper");
        job.setJarByClass(MobileLogMapJob.class);

        //02 设置 input 数据源
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("/mobileaccesslog/data"));
        /*
       [root@hadoop15 ~]# hdfs dfs -cat /mobileaccesslog/data
1363157985066   13726230503     00-FD-07-A4-72-B8:CMCC  120.196.100.82  24      27      2481    24681   200
1363157995052   13826544101     5C-0E-8B-C7-F1-E0:CMCC  120.197.40.4    4       0       264     0       200
1363157991076   13926435656     20-10-7A-28-CC-0A:CMCC  120.196.100.99  2       4       132     1512    200
1363154400022   13926251106     5C-0E-8B-8B-B1-50:CMCC  120.197.40.4    4       0       240     0       200
1363157985066   13726230503     00-FD-07-A4-72-B8:CMCC  120.196.100.82  24      27      2481    24681   200
1363157995052   13826544101     5C-0E-8B-C7-F1-E0:CMCC  120.197.40.4    4       0       264     0       200
1363157991076   13926435656     20-10-7A-28-CC-0A:CMCC  120.196.100.99  2       4       132     1512    200
1363154400022   13926251106     5C-0E-8B-8B-B1-50:CMCC  120.197.40.4    4       0       240     0       200
        */

        // 03 设置mapper
        job.setMapperClass(MobileAccess3Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AccessLog3Writable.class);

        //04 shuffle 无需设置 自动处理

        //05 设置reduce  本示例只是洗数据，所以无需reduce
        job.setNumReduceTasks(0);

        //06 设置output Format
        job.setOutputFormatClass(TextOutputFormat.class);
        Path path = new Path("/mobileaccesslog/result_map");
        FileSystem fileSystem = FileSystem.get(getConf());
        if(fileSystem.exists(path)){
            fileSystem.delete(path,true);
        }
        TextOutputFormat.setOutputPath(job,path);

        boolean status = job.waitForCompletion(true);
        System.out.println("作业执行状态: " + status);

        //验证结果
        /*

        [root@hadoop15 ~]# hdfs dfs -cat /mobileaccesslog/result_map/part-m-00000
13726230503     2481    24681   200
13826544101     264     0       200
13926435656     132     1512    200
13926251106     240     0       200
13726230503     2481    24681   200
13826544101     264     0       200
13926435656     132     1512    200
13926251106     240     0       200

         */

        return 0;
    }
}
