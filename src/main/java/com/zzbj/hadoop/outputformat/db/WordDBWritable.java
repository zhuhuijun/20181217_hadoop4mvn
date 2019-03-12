package com.zzbj.hadoop.outputformat.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class WordDBWritable implements DBWritable, WritableComparable<WordDBWritable> {
	private String word;
	private Integer num;

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public Integer getNum() {
		return num;
	}

	public void setNum(Integer num) {
		this.num = num;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(word);
		out.writeInt(num);
	}

	public void readFields(DataInput in) throws IOException {
		this.word = in.readUTF();
		this.num = in.readInt();
	}

	public void write(PreparedStatement ppst) throws SQLException {
		ppst.setString(1, word);
		ppst.setInt(2, num);
	}

	public void readFields(ResultSet resultSet) throws SQLException {
	}

	public int compareTo(WordDBWritable o) {
		return new Text(this.word).compareTo(new Text(o.word));

	}

}
