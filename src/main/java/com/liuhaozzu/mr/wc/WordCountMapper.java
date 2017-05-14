package com.liuhaozzu.mr.wc;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	// 该方法循环调用，从文件的split碎片段中读取一行，就调用一次；
	// 把该行首字符的下标作为key，该行的内容为value
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("key>>>" + key + "value>>>" + value.toString());
		String[] words = StringUtils.split(value.toString(), ' ');
		System.out.println("words>>>" + Arrays.toString(words));
		for (String w : words) {
			context.write(new Text(w), new IntWritable(1));
			System.out.println("word>>>" + w);
		}
	}
}
