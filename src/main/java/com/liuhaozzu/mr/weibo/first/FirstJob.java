package com.liuhaozzu.mr.weibo.first;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FirstJob {
	private static final Log LOGGER = LogFactory.getLog(FirstJob.class);

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

			job.setJarByClass(FirstJob.class);
			job.setJobName("weibo1");

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			job.setNumReduceTasks(4);
			job.setPartitionerClass(FirstPartition.class);

			job.setMapperClass(FirstMapper.class);
			job.setReducerClass(FirstReducer.class);

			FileInputFormat.addInputPath(job, new Path("/usr/input/wb"));
			Path outputDir = new Path("/usr/output/weibo1");
			if (fs.exists(outputDir)) {
				fs.delete(outputDir, true);
			}
			FileOutputFormat.setOutputPath(job, outputDir);
			boolean f = job.waitForCompletion(true);
			if (f) {
				LOGGER.info("first job run successfully");
				System.out.println("FirstMapper>>>" + FirstMapper.MAP);
				System.out.println("FirstReducer>>>" + FirstReducer.MAP);
			}
		} catch (Exception e) {
			LOGGER.error(e);
		}
	}

}
