package com.liuhaozzu.mr.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySort extends WritableComparator {
	public MySort() {
		super(MyKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MyKey k1 = (MyKey) a;
		MyKey k2 = (MyKey) b;
		// 降序排列
		return k2.compareTo(k1);
	}
}
