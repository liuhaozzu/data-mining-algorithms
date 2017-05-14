package com.liuhaozzu.mr.weibo.first;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sun.jersey.client.impl.CopyOnWriteHashMap;

public class FirstReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private static final Log LOGGER = LogFactory.getLog(FirstReducer.class);
	public static final Map<Text, Iterable<IntWritable>> MAP = new CopyOnWriteHashMap<>();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		MAP.put(key, values);
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		if (key.equals(new Text("count"))) {
			LOGGER.info(key.toString() + "___________" + sum);
		}
		context.write(key, new IntWritable(sum));
	}

}
