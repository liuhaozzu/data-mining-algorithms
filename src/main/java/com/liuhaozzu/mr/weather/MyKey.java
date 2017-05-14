package com.liuhaozzu.mr.weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MyKey implements WritableComparable<MyKey> {

	private static final long serialVersionUID = 3022813267664623214L;
	private int year;
	private int month;
	private double hot;

	public MyKey() {
	}

	public MyKey(int year, int month, double hot) {
		this.year = year;
		this.month = month;
		this.hot = hot;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public double getHot() {
		return hot;
	}

	public void setHot(double hot) {
		this.hot = hot;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(hot);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + month;
		result = prime * result + year;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MyKey other = (MyKey) obj;
		if (Double.doubleToLongBits(hot) != Double.doubleToLongBits(other.hot))
			return false;
		if (month != other.month)
			return false;
		if (year != other.year)
			return false;
		return true;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.year);
		out.writeInt(this.month);
		out.writeDouble(this.hot);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.month = in.readInt();
		this.hot = in.readDouble();
	}

	// 当该对象作为输出的key的时候，判断对象是否是同一个对象
	@Override
	public int compareTo(MyKey o) {
		int r1 = Integer.compare(this.year, o.year);
		if (r1 == 0) {
			int r2 = Integer.compare(this.month, o.month);
			if (r2 == 0) {
				return Double.compare(this.hot, o.hot);
			} else {
				return r2;
			}
		} else {
			return r1;
		}
	}

}
