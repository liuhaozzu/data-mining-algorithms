package com.liuhaozzu.mr.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator {
	public MyGroup() {
		super(MyKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MyKey k1 = (MyKey) a;
		MyKey k2 = (MyKey) b;
		int r1 = Integer.compare(k1.getYear(), k2.getYear());
		if (r1 == 0) {
			int r2 = Integer.compare(k1.getMonth(), k2.getMonth());
			return r2;
		} else {
			return r1;
		}
	}
}
