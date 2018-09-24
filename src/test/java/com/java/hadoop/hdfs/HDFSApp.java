package com.java.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/*
    Hadoop HDFS Java API 操作
 */
public class HDFSApp {
    public static  final  String  HDFS_PARH = "hdfs://192.168.92.133:8020";
    FileSystem fileSystem = null;
    Configuration configuration =null;
    @Before
    public void setUp() throws  Exception{
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PARH),configuration,"root");
    }
    @After
    public  void tearDown() throws Exception{
        configuration = null;
        fileSystem = null;
    }
    /*
    * HDFS 创建文件夹
    * */
    @Test
    public void mkdir() throws  Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }
    /*创建文件
    * */
    @Test
    public void create() throws Exception{
        try {
            FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
            outputStream.write("hello hadoop".getBytes());
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**查看HDFS文件内容
     * */
    @Test
    public void  cat() throws  Exception{
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(fsDataInputStream,System.out,1024);
        fsDataInputStream.close();
    }
}
