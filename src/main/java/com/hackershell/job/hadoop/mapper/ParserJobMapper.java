package com.hackershell.job.hadoop.mapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;

import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

public class ParserJobMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text> {

    private static final Log LOG = LogFactory.getLog(ParserJobMapper.class);
    private static JobConf conf;


    public void map(Object key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        Text word = new Text("");
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            FileSystem fs = FileSystem.get(URI.create(word.toString()), conf);
            FileStatus[] stat = fs.listStatus(new Path(word.toString()));
            for (FileStatus f : stat) {
               if (JobHistoryUtils.isValidJobHistoryFileName(f.getPath().toString())) {
                   output.collect(word, new Text(f.getPath().toString()));
               }
            }
        }
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        conf = job;
    }
}
