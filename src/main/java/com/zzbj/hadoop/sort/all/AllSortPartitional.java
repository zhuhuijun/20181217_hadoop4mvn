package com.zzbj.hadoop.sort.all;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/***
 * 全区排序函数
 * 
 * @author zhuhuijun
 *
 */
public class AllSortPartitional extends Partitioner<IntWritable, IntWritable> {

	/***
	 * 获得分区
	 */
	public int getPartition(IntWritable key, IntWritable value, int num) {
		int step = 10 / num;
		// 判断年份落在那个区间
		int[][] arr = new int[num][2];
		for (int i = 0; i < num; i++) {
			if (i == 0) {
				arr[0] = new int[] { Integer.MIN_VALUE, 1901 + step * (i + 1) };
			} else if (i == num - 1) {
				arr[i] = new int[] { 1901 + step * i, Integer.MAX_VALUE };
			} else {
				arr[i] = new int[] { 1901 + step * i, 1901 + step * (i + 1) };
			}
		}

		int year = key.get();
		for (int j = 0; j < arr.length; j++) {
			int[] row = arr[j];
			if (year >= row[0] && year < row[1]) {
				return j;
			}
		}
		return 0;
	}

}
