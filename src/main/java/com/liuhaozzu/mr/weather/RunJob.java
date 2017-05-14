package com.liuhaozzu.mr.weather;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
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

			job.setJobName("weather");
			job.setMapperClass(WeatherMapper.class);
			job.setReducerClass(WeatherReducer.class);

			job.setMapOutputKeyClass(MyKey.class);
			job.setMapOutputValueClass(DoubleWritable.class);

			job.setPartitionerClass(MyPartitioner.class);
			job.setSortComparatorClass(MySort.class);
			job.setGroupingComparatorClass(MyGroup.class);

			job.setNumReduceTasks(3);
			job.setInputFormatClass(KeyValueTextInputFormat.class);

			FileInputFormat.addInputPath(job, new Path("/usr/input/weather"));
			// 该目录不能已经存在
			Path outpath = new Path("/usr/output/weather");

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

	// 每一行的第一个隔开符左边为key，右边为value
	static class WeatherMapper extends Mapper<Text, Text, MyKey, DoubleWritable> {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, MyKey, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			try {
				Date date = sdf.parse(key.toString());
				Calendar c = Calendar.getInstance();
				c.setTime(date);
				int year = c.get(Calendar.YEAR);
				int month = c.get(Calendar.MONTH);
				double hot = Double.parseDouble(value.toString().substring(0, value.toString().lastIndexOf('c')));
				MyKey myKey = new MyKey(year, month, hot);
				context.write(myKey, new DoubleWritable(hot));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	static class WeatherReducer extends Reducer<MyKey, DoubleWritable, Text, NullWritable> {

		@Override
		protected void reduce(MyKey arg0, Iterable<DoubleWritable> arg1,
				Reducer<MyKey, DoubleWritable, Text, NullWritable>.Context arg2)
				throws IOException, InterruptedException {
			int i = 0;
			for (DoubleWritable v : arg1) {
				i++;
				String msg = arg0.getYear() + "\t" + arg0.getMonth() + "\t" + v.get();
				arg2.write(new Text(msg), NullWritable.get());
				if (i >= 3) {
					break;
				}
			}

		}
	}
}
