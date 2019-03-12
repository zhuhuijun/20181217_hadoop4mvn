package com.zzbj.hadoop.sort.secondary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/***
 * 组合key解决二次排序的问题
 * 
 * @author zhuhuijun
 *
 */
public class CombineKey implements WritableComparable<CombineKey> {

	private int year;
	private int temp;

	public CombineKey(int year, int temp) {
		this.year = year;
		this.temp = temp;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getTemp() {
		return temp;
	}

	public void setTemp(int temp) {
		this.temp = temp;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(this.year);
		out.writeInt(this.temp);
	}

	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.temp = in.readInt();
	}

	/***
	 * 气温降序，年份升序的比较
	 */
	public int compareTo(CombineKey o) {
		int oyear = o.getYear();
		int otemp = o.getTemp();
		if (this.year != oyear) {
			return this.year - oyear;
		} else {
			return otemp - this.temp;
		}
	}

}
