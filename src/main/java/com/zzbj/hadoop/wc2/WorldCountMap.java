package com.zzbj.hadoop.wc2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.zzbj.hadoop.mr.UtilHelper;

/****
 * 创建mapper类
 * 
 * @author zhuhuijun
 *
 */

public class WorldCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	/**
	 * wordsMap
	 */
	private Map<String, Integer> wordsMap;

	/**
	 * setup
	 */
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		wordsMap = new HashMap<String, Integer>();
	}

	/***
	 * map
	 */
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		if (wordsMap.size() >= 10000) {
			sendDataAndClean(context);
		}
		String line = value.toString();
		String[] arr = line.split(" ");
		for (String word : arr) {
			addToMap(word);
		}
		context.getCounter("m", UtilHelper.GetGrp2("WcMapper.map", this.hashCode())).increment(1);
	}

	/**
	 * cleanup
	 */
	protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		/**
		 * 最后必须发送一下
		 */
		sendDataAndClean(context);
	}

	/***
	 * 添加单词到集合中
	 * 
	 * @param word
	 */
	private void addToMap(String word) {
		Integer count = wordsMap.get(word);
		if (count == null) {
			count = 1;
		} else {
			count++;
		}
		wordsMap.put(word, count);
	}

	/**
	 * 发送数据
	 * 
	 * @param context
	 */
	private void sendDataAndClean(Mapper<LongWritable, Text, Text, IntWritable>.Context context) {
		try {
			Text text = new Text();
			IntWritable count = new IntWritable();
			for (Map.Entry<String, Integer> entry : wordsMap.entrySet()) {
				String key = entry.getKey();
				Integer value = entry.getValue();
				text.set(key);
				count.set(value);
				context.write(text, count);
			}
			wordsMap.clear();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
