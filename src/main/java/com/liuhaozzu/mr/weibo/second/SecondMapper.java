package com.liuhaozzu.mr.weibo.second;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.sun.jersey.client.impl.CopyOnWriteHashMap;

//统计df:词在多少个微博中出现过
public class SecondMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	// for test
	private static final Map<LongWritable, Text> map = new CopyOnWriteHashMap<>();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		map.put(key, value);
		// 获取当前mapper task的数据片段
		FileSplit fs = (FileSplit) context.getInputSplit();

		if (!fs.getPath().getName().contains("part-r-00003")) {
			String[] v = value.toString().trim().split("\t");
			if (v.length >= 2) {
				String[] ss = v[0].split("_");
				if (ss.length >= 2) {
					String w = ss[0];
					context.write(new Text(w), new IntWritable(1));
				}
			} else {
				System.out.println(value.toString() + "-------------");
				System.err.println(map);
			}
		}
	}
}
