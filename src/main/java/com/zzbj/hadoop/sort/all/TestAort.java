package com.zzbj.hadoop.sort.all;

public class TestAort {

	public static void main(String[] args) {
		int num = 6;
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
		System.out.println();
	}

}
