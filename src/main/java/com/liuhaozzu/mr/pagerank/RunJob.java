package com.liuhaozzu.mr.pagerank;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {
	private static Log LOGGER = LogFactory.getLog(RunJob.class);

	public static enum MyCounter {
		My
	}

	public static void main(String[] args) {
		double d = 0.001;
		Configuration conf = new Configuration();
		// YARNRunner
		/*
		 * conf.set("fs.defaultFS", "hdfs://hadoop1:8020");
		 * conf.set("yarn.resourcemanager.hostname", "hadoop1");
		 */
		conf.set("mapred.jar",
				"E:\\workspaces\\sts-3.8.3\\hadoop-wordcount\\target\\hadoop-wordcount-0.0.1-SNAPSHOT.jar");
		int i = 0;
		while (true) {
			i++;
			try {
				conf.setInt("runCount", i);
				FileSystem fs = FileSystem.get(conf);
				Job job = Job.getInstance(conf);
				job.setJarByClass(RunJob.class);

				job.setJobName("pr" + i);

				job.setMapperClass(PageRankMapper.class);
				job.setReducerClass(PageRankReducer.class);

				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);

				job.setInputFormatClass(KeyValueTextInputFormat.class);

				Path inputPath = new Path("/usr/input/pagerank.txt");
				if (i > 1) {
					inputPath = new Path("/usr/output/pr" + (i - 1));
				}

				FileInputFormat.addInputPath(job, inputPath);

				// 该目录不能已经存在
				Path outpath = new Path("/usr/output/pr" + i);

				if (fs.exists(outpath)) {
					fs.delete(outpath, true);
				}
				FileOutputFormat.setOutputPath(job, outpath);
				boolean f = job.waitForCompletion(true);
				if (f) {
					LOGGER.info("job-pr" + i + "任务执行成功");
					long sum = job.getCounters().findCounter(MyCounter.My).getValue();
					double avgd = sum / 4000.0;
					if (avgd < d) {
						break;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	// 每一行的第一个隔开符左边为key，右边为value
	static class PageRankMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int runCount = context.getConfiguration().getInt("runCount", 1);
			String page = key.toString();
			Node node = null;
			if (runCount == 1) {
				node = Node.fromMR("1.0" + "\t" + value.toString());
			} else {
				node = Node.fromMR(value.toString());
			}
			context.write(new Text(page), new Text(node.toString()));
			if (node.containsAdjacentNodes()) {
				double outValue = node.getPageRank() / node.getAdjacentNodeNames().length;
				for (int i = 0; i < node.getAdjacentNodeNames().length; i++) {
					String outPage = node.getAdjacentNodeNames()[i];
					context.write(new Text(outPage), new Text(Double.toString(outValue)));// B:0.5
																							// D:0.5
				}
			}
		}
	}

	static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1, Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			double sum = 0.0;
			Node sourceNode = null;
			for (Text i : arg1) {
				Node node = Node.fromMR(i.toString());
				if (node.containsAdjacentNodes()) {
					sourceNode = node;
				} else {
					sum += node.getPageRank();
				}
			}
			double newPR = (0.15 / 4) + 0.85 * sum;
			LOGGER.info("************** new pageRank value is: " + newPR);

			if (sourceNode != null) {
				// 把新的pr值和计算之前的pr值比较
				double d = newPR - sourceNode.getPageRank();
				int j = (int) (d * 1000);
				j = Math.abs(j);
				LOGGER.info(j + "----------------------------");
				arg2.getCounter(MyCounter.My).increment(j);
				sourceNode.setPageRank(newPR);
				arg2.write(arg0, new Text(sourceNode.toString()));
			}
		}
	}
}
