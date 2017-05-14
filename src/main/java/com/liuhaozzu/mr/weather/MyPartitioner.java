package com.liuhaozzu.mr.weather;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MyPartitioner extends HashPartitioner<MyKey, DoubleWritable> {

	// Map task 每输出一个数据就调用一次
	// 所以执行时间越短越好
	@Override
	public int getPartition(MyKey key, DoubleWritable value, int numReduceTasks) {
		return (key.getYear() - 1949) % numReduceTasks;
	}
}
