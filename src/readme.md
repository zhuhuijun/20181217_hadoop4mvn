#### SequenceFile

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