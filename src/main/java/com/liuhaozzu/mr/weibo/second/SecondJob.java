package com.liuhaozzu.mr.weibo.second;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondJob {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		/*
		 * conf.set("fs.defaultFS", "hdfs://hadoop2:8020");
		 * conf.set("yarn.resourcemanager.hostname", "hadoop2");
		 */

		conf.set("mapred.jar",
				"E:\\workspaces\\sts-3.8.3\\hadoop-wordcount\\target\\hadoop-wordcount-0.0.1-SNAPSHOT.jar");

		try {
			Job job = Job.getInstance(conf);
			job.setJarByClass(SecondJob.class);
			job.setJobName("weibo2");
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			job.setMapperClass(SecondMapper.class);
			job.setReducerClass(SecondReducer.class);
			job.setCombinerClass(SecondReducer.class);

			// mr运行时的输入数据从hdfs的哪个目录中获取
			FileInputFormat.addInputPath(job, new Path("/usr/output/weibo1"));
			FileOutputFormat.setOutputPath(job, new Path("/usr/output/weibo2"));
			boolean f = job.waitForCompletion(true);
			if (f) {
				System.out.println("second job 执行成功");
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}
