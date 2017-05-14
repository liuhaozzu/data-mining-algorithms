package com.liuhaozzu.mr.wc2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce类：自己定义reduce方法
 */
public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	/**
	 * Reducer类中的reduce方法： protected void reduce(KEYIN key, Interable<VALUEIN>
	 * value, Context context) 映射一个单个的输入k/v对到一个中间的k/v对
	 * Context类：收集Reducer输出的<k,v>对。
	 */
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		context.write(key, new IntWritable(sum));
	}
}