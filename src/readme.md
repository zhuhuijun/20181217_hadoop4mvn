#### 1.SequenceFile

```

查看文件的内容 hdfs dfs -text /user/mysequencefile.seq

public static void Write() throws Exception
{
	Configuration conf = new Configuration();
	conf.set("fs.defaultFS", "hdfs://s100:8020");
	FileSystem fs = FileSystem.get(conf);
	Path path = new Path("hdfs://s100:8020/user/ubuntu/mysequencefile.seq");
	Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);
	for (int i = 1; i < 10; i++)
	{
		IntWritable key = new IntWritable(i);
		Text val = new Text("tom:" + i);
		writer.append(key, val);
	}
	writer.close();
	System.out.println("over!!!");

}
	
	

```


#### 2.MapFile

```
hdfs dfs -text /user/ubuntu/mymap.map/index
hdfs dfs -text /user/ubuntu/mymap.map/data

添加时key按照小到大的顺序添加
index:索引和偏移量的映射可以设置间隔默认的间隔大小是128
通过配置文件io.map.index.interval设置间隔值，可以进行折半查找

可以寻找与指定key相近的key查找，可以设置向前查找，默认是向后查找


```



#### 3.IPC

```
进程间的通信
RPC：remote  procedure call 远程
特点： 紧凑 快速 可扩展 这是他的特点
hadoop字节的序列化格式Writable

public interface Writable {
  /** 
   * Serialize the fields of this object to <code>out</code>.
   * 
   * @param out <code>DataOuput</code> to serialize this object into.
   * @throws IOException
   */
  void write(DataOutput out) throws IOException;

  /** 
   * Deserialize the fields of this object from <code>in</code>.  
   * 
   * <p>For efficiency, implementations should attempt to re-use storage in the 
   * existing object where possible.</p>
   * 
   * @param in <code>DataInput</code> to deseriablize this object from.
   * @throws IOException
   */
  void readFields(DataInput in) throws IOException;
}
```