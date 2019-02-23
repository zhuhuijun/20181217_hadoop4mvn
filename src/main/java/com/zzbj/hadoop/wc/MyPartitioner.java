package com.zzbj.hadoop.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable> {

	/***
	 * 自定义分区函数
	 */
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		return 0;
	}

}
