package com.liuhaozzu.mr.friendsrecommend;

import org.apache.hadoop.io.Text;

public class FoF extends Text {
	public FoF() {
		super();
	}

	public FoF(String a, String b) {
		super(getFoF(a, b));
	}

	public static String getFoF(String a, String b) {
		int r = a.compareTo(b);
		if (r < 0) {
			return a + "\t" + b;
		} else {
			return b + "\t" + a;
		}
	}
}
