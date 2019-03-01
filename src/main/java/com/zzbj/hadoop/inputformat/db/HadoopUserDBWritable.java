package com.zzbj.hadoop.inputformat.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/***
 *
 * @author zhuhuijun
 *
 */
public class HadoopUserDBWritable implements DBWritable, Writable {

    private int id;
    private String name;
    private int age;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    //hadoop的串行化
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(name);
        out.writeInt(age);
    }

   /**
    * fan 序列化
    */
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.name = in.readUTF();
        this.age = in.readInt();
    }

    public void write(PreparedStatement ppst) throws SQLException {
        //TODO
    }

    public void readFields(ResultSet resultSet) throws SQLException {
    	this.id = resultSet.getInt("id");
    	this.name = resultSet.getString("name");
    	this.age = resultSet.getInt("age");
    }

}
