package com.liuhaozzu.mr.weibo.third;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		for (Text i : values) {
			sb.append(i.toString() + "\t");
		}
		context.write(key, new Text(sb.toString()));
	}
}
