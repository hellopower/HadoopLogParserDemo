# HadoopLogParserDemo
利用MR解析job日志demo

1.先生成jobpathList文件

2.每一行用一个map处理，找出合适的文件

3.reduce进行数据解析

使用：

hadoop jar com.hackershell.job.hadoop.HadoopLogParser ns2 hdfs://ns2/user/history/done/2015/08/12/ hdfs://ns2/tmp/job-54
