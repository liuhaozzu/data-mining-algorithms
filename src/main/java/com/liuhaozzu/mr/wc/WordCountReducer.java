package com.liuhaozzu.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	// 每一组的数据调用一次；数据特点：每一组的key相同，value有可能有多个
	@Override
	protected void reduce(Text text, Iterable<IntWritable> iterable,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		System.out.println("Text>>>" + text);
		for (IntWritable i : iterable) {
			sum += i.get();
		}
		System.out.println("text to write>>>" + sum);
		context.write(text, new IntWritable(sum));
	}
}
