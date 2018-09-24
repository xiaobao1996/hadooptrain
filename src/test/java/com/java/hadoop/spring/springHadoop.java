package com.java.hadoop.spring;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class springHadoop {
private ApplicationContext ctx;
private FileSystem fileSystem;
/**
 * 创建HDFS文件夹
 */
@Test
public void testMkdirs() throws Exception{
    fileSystem.mkdirs(new Path("/springhdfs/"));
}

public void testRead() throws  Exception{
    FSDataInputStream in = fileSystem.open(new Path("/test.txt"));
    IOUtils.copyBytes(in,System.out,1024);
    in.close();
}
@Before
public void setUp(){
    ctx = new ClassPathXmlApplicationContext("beans.xml");
    fileSystem = (FileSystem)ctx.getBean("fileSystem");
}
@After
public void tearDown() throws Exception{
    ctx =null;
    fileSystem = null;
}
}
