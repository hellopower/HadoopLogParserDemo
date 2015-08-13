package com.hackershell.job.hadoop.reducer;

import com.hackershell.job.hadoop.jobhistory.ParserJobService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;


@SuppressWarnings("deprecation")
public class ParserJobReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private static final Log LOG = LogFactory.getLog(ParserJobReducer.class);
    private static JobConf conf;

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
            while (values.hasNext()) {
                String path = values.next().toString();
                FileSystem fs = FileSystem.get(URI.create(path), conf);
                ParserJobService parserJobService = new ParserJobService(fs, new Path(path));
                String jobDetail = parserJobService.getJobDetail();
                Text jobDetailText = new Text(jobDetail);
                output.collect(new Text(""), jobDetailText);
            }

    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        conf = job;
    }
}
