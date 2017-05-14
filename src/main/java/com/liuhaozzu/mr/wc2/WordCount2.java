package com.liuhaozzu.mr.wc2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 描述：WordCount explains by xxm
 * 
 * @author xxm
 */
public class WordCount2 {

	/**
	 * main主函数
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();// 创建一个配置对象，用来实现所有配置
		conf.set("mapred.jar",
				"E:\\workspaces\\sts-3.8.3\\hadoop-wordcount\\target\\hadoop-wordcount-0.0.1-SNAPSHOT.jar");
		FileSystem fs = FileSystem.get(conf);
		Job job = new Job(conf, "wordcount2");// 新建一个job，并定义名称

		job.setOutputKeyClass(Text.class);// 为job的输出数据设置Key类
		job.setOutputValueClass(IntWritable.class);// 为job输出设置value类

		job.setMapperClass(WCMapper.class); // 为job设置Mapper类
		job.setReducerClass(WCReducer.class);// 为job设置Reduce类
		job.setJarByClass(WordCount2.class);

		job.setInputFormatClass(TextInputFormat.class);// 为map-reduce任务设置InputFormat实现类
		job.setOutputFormatClass(TextOutputFormat.class);// 为map-reduce任务设置OutputFormat实现类

		FileInputFormat.addInputPath(job, new Path("/usr/input/"));// 为map-reduce
		Path outpath = new Path("/usr/output/wc");

		if (fs.exists(outpath)) {
			fs.delete(outpath, true);
		} // job设置输入路径
		FileOutputFormat.setOutputPath(job, outpath);// 为map-reduce
														// job设置输出路径
		job.waitForCompletion(true); // 运行一个job，并等待其结束
	}

}