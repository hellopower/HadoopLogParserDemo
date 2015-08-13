package com.jd.bdp.hadoop;

import com.jd.bdp.hadoop.mapper.ParserJobMapper;
import com.jd.bdp.hadoop.reducer.ParserJobReducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;


public class HadoopLogParser implements Tool {

    private static final Log LOG = LogFactory.getLog(HadoopLogParser.class);
    private static final String TMP_FILE = "/tmp/job_input";

    private Configuration base;

    public HadoopLogParser(Configuration base) {
        this.base = base;
    }

    public static void main(String[] args) throws Exception {
        Configuration startCfg = new Configuration(true);
        HadoopLogParser runner = new HadoopLogParser(startCfg);
        int ec = ToolRunner.run(runner, args);
        System.exit(ec);

    }

    @Override
    public int run(String[] args) throws Exception {

        String ns = args[0];
        String inputDir = args[1];
        String outputDir = args[2];
        LOG.info("HadoopLogParser的输入路径为" + inputDir);
        LOG.info("HadoopLogParser的输出路径为" + outputDir);

        runJob(getConf(), initJobPath(inputDir), getNSTMP(ns), outputDir);
        return 0;
    }

    private String getNSTMP(String ns) {
        return "hdfs://" + ns + TMP_FILE;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.base = configuration;
    }

    @Override
    public Configuration getConf() {
        return base;
    }

    private void print() {

    }

    private JobConf getJob(Configuration conf, int redNum, String in, String out) {
        JobConf job = new JobConf(conf, HadoopLogParser.class);
        job.setJobName("HadoopLogParserDemo");
        job.setMapperClass(ParserJobMapper.class);
        job.setReducerClass(ParserJobReducer.class);
        job.setInputFormat(NLineInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormat(TextOutputFormat.class);
        TextOutputFormat.setCompressOutput(job, false);
        FileOutputFormat.setOutputPath(job, new Path(out));
        FileInputFormat.setInputPaths(job, in);
        job.setNumReduceTasks(redNum);

        return job;
    }

    private void runJob(Configuration conf, int redNum, String in, String out) throws IOException {
        JobClient.runJob(getJob(conf, redNum, in, out));
    }

    private String getString(Path[] paths) {
        StringBuffer sb = new StringBuffer();
        for(Path path: paths) {
            sb.append(path.toString()).append("\n");
        }
        return sb.toString();
    }

    private int initJobPath(String inputDir) throws IOException {

        FileSystem fs = FileSystem.get(URI.create(inputDir), getConf());
        if (fs.exists(new Path(TMP_FILE))) {
            fs.delete(new Path(TMP_FILE), true);
        }

        FileStatus[] dirStat = fs.listStatus(new Path(inputDir));
        Path[] paths = new Path[dirStat.length];
        for (int i = 0; i < dirStat.length; i++) {
            paths[i] = dirStat[i].getPath();
            LOG.debug("path: " + paths[i]);
        }

        FSDataOutputStream fout = fs.create(new Path(TMP_FILE));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fout,"UTF-8"));
        out.write(getString(paths));
        out.flush();

        if (out != null) {
            out.close();
        }

        return paths.length;
    }
}
