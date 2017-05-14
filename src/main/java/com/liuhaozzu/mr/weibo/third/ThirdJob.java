package com.liuhaozzu.mr.weibo.third;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ThirdJob {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		/*
		 * conf.set("fs.defaultFS", "hdfs://hadoop2:8020");
		 * conf.set("yarn.resourcemanager.hostname", "hadoop2");
		 */

		conf.set("mapred.jar",
				"E:\\workspaces\\sts-3.8.3\\hadoop-wordcount\\target\\hadoop-wordcount-0.0.1-SNAPSHOT.jar");

		try {
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			job.setJarByClass(ThirdJob.class);
			job.setJobName("weibo3");

			// 把微博总数加载到内存
			job.addCacheFile(new Path("/usr/output/weibo1/part-r-00003").toUri());
			// 把df加载到内存
			job.addCacheFile(new Path("/usr/output/weibo2/part-r-00000").toUri());

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(ThirdMapper.class);
			job.setReducerClass(ThirdReducer.class);

			// mr运行时的输入数据从hdfs的哪个目录中获取
			FileInputFormat.addInputPath(job, new Path("/usr/output/weibo1"));
			Path outputDir = new Path("/usr/output/weibo3");
			if (fs.exists(outputDir)) {
				fs.delete(outputDir, true);
			}
			FileOutputFormat.setOutputPath(job, outputDir);

			boolean f = job.waitForCompletion(true);
			if (f) {
				System.out.println("second job 执行成功");
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}
