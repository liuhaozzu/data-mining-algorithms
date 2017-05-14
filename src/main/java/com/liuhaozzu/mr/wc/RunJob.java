package com.liuhaozzu.mr.wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		// YARNRunner
		/*
		 * conf.set("fs.defaultFS", "hdfs://hadoop1:8020");
		 * conf.set("yarn.resourcemanager.hostname", "hadoop1");
		 */
		conf.set("mapred.jar",
				"E:\\workspaces\\sts-3.8.3\\hadoop-wordcount\\target\\hadoop-wordcount-0.0.1-SNAPSHOT.jar");
		try {
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			job.setJarByClass(RunJob.class);

			job.setJobName("wordcount");
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path("/usr/input/"));
			// 该目录不能已经存在
			Path outpath = new Path("/usr/output/wc");

			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);
			boolean f = job.waitForCompletion(true);
			if (f) {
				System.out.println("job任务执行成功");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
