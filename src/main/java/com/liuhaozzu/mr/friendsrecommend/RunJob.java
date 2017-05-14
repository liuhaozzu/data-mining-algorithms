package com.liuhaozzu.mr.friendsrecommend;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {
	private static final Log LOGGER = LogFactory.getLog(RunJob.class);

	public static void main(String[] args) {
		Configuration conf = new Configuration();

		conf.set("mapred.jar",
				"E:\\workspaces\\sts-3.8.3\\hadoop-wordcount\\target\\hadoop-wordcount-0.0.1-SNAPSHOT.jar");
		boolean result = run1(conf);
		if (result) {
			run2(conf);
		}
	}

	private static void run2(Configuration conf) {
		try {
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			job.setJarByClass(RunJob.class);

			job.setJobName("fof2");

			job.setMapperClass(SortMapper.class);
			job.setReducerClass(SortReducer.class);
			job.setSortComparatorClass(FoFSort.class);
			job.setGroupingComparatorClass(FoFGroup.class);
			job.setMapOutputKeyClass(User.class);
			job.setMapOutputValueClass(User.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);

			FileInputFormat.addInputPath(job, new Path("/usr/output/friends"));
			Path outputPath = new Path("/usr/output/f2");
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(job, outputPath);
			boolean f = job.waitForCompletion(true);
			if (f) {
				System.out.println("job 执行成功");
			}
		} catch (Exception e) {
			// TODO: handle exception
		}

	}

	public static boolean run1(Configuration conf) {

		try {
			FileSystem fs = FileSystem.get(conf);
			Job job = Job.getInstance(conf);
			job.setJarByClass(RunJob.class);

			job.setJobName("friendrecommend");

			job.setMapperClass(FoFMapper.class);
			job.setReducerClass(FoFReducer.class);
			job.setMapOutputKeyClass(FoF.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setInputFormatClass(KeyValueTextInputFormat.class);

			FileInputFormat.addInputPath(job, new Path("/usr/input/friend"));
			// 该目录不能已经存在
			Path outpath = new Path("/usr/output/friends");

			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);
			boolean f = job.waitForCompletion(true);
			if (f) {
				System.out.println("job任务执行成功");
				return f;
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
		return false;
	}

	static class FoFMapper extends Mapper<Text, Text, FoF, IntWritable> {
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, FoF, IntWritable>.Context context)
				throws IOException, InterruptedException {
			LOGGER.error(key.toString());
			LOGGER.error(value.toString());
			String user = key.toString();
			String[] friends = StringUtils.split(value.toString(), '\t');
			for (int i = 0; i < friends.length; i++) {
				String f = friends[i];
				FoF oFoF = new FoF(user, f);
				context.write(oFoF, new IntWritable(0));
				for (int j = i + 1; j < friends.length; j++) {
					String f2 = friends[j];
					FoF foF = new FoF(f, f2);
					context.write(foF, new IntWritable(1));

				}
			}
		}
	}

	static class FoFReducer extends Reducer<FoF, IntWritable, FoF, IntWritable> {

		@Override
		protected void reduce(FoF key, Iterable<IntWritable> values,
				Reducer<FoF, IntWritable, FoF, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum = 0;
			boolean f = true;
			for (IntWritable i : values) {
				if (i.get() == 0) {
					f = false;
					continue;
				} else {
					sum += i.get();
				}
			}
			if (f) {
				context.write(key, new IntWritable(sum));
			}
		}
	}

	static class SortMapper extends Mapper<Text, Text, User, User> {
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, User, User>.Context context)
				throws IOException, InterruptedException {
			String[] args = StringUtils.split(value.toString(), '\t');
			String other = args[0];
			int friendsCount = Integer.parseInt(args[1]);
			context.write(new User(key.toString(), friendsCount), new User(other, friendsCount));
			context.write(new User(other, friendsCount), new User(key.toString(), friendsCount));
		}
	}

	static class SortReducer extends Reducer<User, User, Text, Text> {
		@Override
		protected void reduce(User key, Iterable<User> values, Reducer<User, User, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String user = key.getUname();
			StringBuffer sb = new StringBuffer();
			for (User u : values) {
				sb.append(u.getUname() + ":" + u.getFriendsCount());
				sb.append(",");
			}
			context.write(new Text(user), new Text(sb.toString()));
		}
	}
}
