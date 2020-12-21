package com.tiza.leo.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Author: tz_wl
 * Date: 2020/12/17 10:22
 * Content:
 *
 * https://blog.csdn.net/wei198621/article/details/111280555
 *
 */
public class Test01 {
    private FileSystem fileSystem ;   //hdfs客户端对象

    @Before
    public void before() throws IOException {
        //hadoop文件系统的权限设置为root  ,不设置，使用默认的登录用户
        System.setProperty("HADOOP_USER_NAME","root");
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS","hdfs://192.168.213.201:9000");
        //conf.set("dfs.replication","1");
        conf.set("fs.defaultFS","hdfs://192.168.213.215:9000");
        fileSystem = FileSystem.get(conf);
    }


    @After
    public void close() throws IOException {
        //
        fileSystem.close();
    }


    //4.2.3 上传文件到hdfs
    @Test
    public void testUpload() throws IOException {
        //  上传本地文件到 hadoop 服务器
        FileInputStream is = new FileInputStream("E:\\tempData\\20201217.txt");
        Path path = new Path("/now.txt");
        FSDataOutputStream os = fileSystem.create(path);
        IOUtils.copyBytes(is,os,1024,true);
    }

    // 4.2.4 hdfs下载文件
    // 2.第一种方式  --- failed ， HADOOP_HOME and hadoop.home.dir are unset.  window 操作的原因
    @Test
    public void TestDownload1() throws IOException {
        Path source = new Path("/now.txt");
        Path dest = new Path("E:\\tempData\\download");
        fileSystem.copyToLocalFile(source,dest);
    }

    // 2.第二种方式  -- ok
    @Test
    public void testDownload2() throws IOException {
        Path path = new Path("/now.txt");
        FSDataInputStream in = fileSystem.open(path);
        FileOutputStream os = new FileOutputStream("E:\\tempData\\download\\now.txt");
        IOUtils.copyBytes(in,os,1024,true);
    }

    //4.2.5 hdfs创建目录
    @Test
    public void testMkdir() throws IOException {
        Path path = new Path("/aa/bb/cc");
        boolean b = fileSystem.mkdirs(path);
        System.out.println("mkdir  success="+ b);
    }

    //4.2.6 展示hdfs文件列表
    @Test
    public void testListFiles() throws IOException {
        Path path = new Path("/");
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(path, true); //true  递归展示
        while (listFiles.hasNext()){
            LocatedFileStatus next = listFiles.next();
            System.out.println("next = " + next);
        }
    }

    // 4.2.7 展示hdfs目录和文件
    @Test
    public void testListDirs() throws IOException {
        Path path = new Path("/");
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println("fileStatus directory = " + fileStatus.isDirectory() + " ; path is "+ fileStatus.getPath());
        }
    }

    //4.2.8 删除文件
    @Test
    public void testDelete() throws IOException {
        Path path = new Path("/aa");
        fileSystem.delete(path, true);

    }

}
