package com.hackershell.job.hadoop.jobhistory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.JobInfo;
import org.apache.hadoop.util.StringUtils;

public class ParserJobService {

    private static final Log LOG = LogFactory.getLog(ParserJobService.class);
    private  JobHistoryParser parser;
    private  JobInfo job;

    public final static SimpleDateFormat dateFormat = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss");


    private final static String FILE_BYTES_READ = "FILE_BYTES_READ";
    private final static String FILE_BYTES_WRITTEN = "FILE_BYTES_WRITTEN";
    private final static String HDFS_BYTES_READ = "HDFS_BYTES_READ";
    private final static String HDFS_BYTES_WRITTEN = "HDFS_BYTES_WRITTEN";
    private final static String HDFS_READ_OPS = "HDFS_READ_OPS";
    private final static String HDFS_WRITE_OPS = "HDFS_WRITE_OPS";
    private final static String CPU_TIME_SPENT = "CPU_MILLISECONDS";
    private final static String GC_TIME_MILLIS = "GC_TIME_MILLIS";
    private final static String PHYSICAL_MEMORY_BYTES = "PHYSICAL_MEMORY_BYTES";
    private final static String COMMITTED_HEAP_BYTES = "COMMITTED_HEAP_BYTES";
    private final static String VIRTUAL_MEMORY_BYTES = "VIRTUAL_MEMORY_BYTES";
    private final static String NULL = "NULL";

    public ParserJobService(FileSystem fs, Path jobHistoryFile ) throws IOException {
        FSDataInputStream in = fs.open(jobHistoryFile);
        parser = new JobHistoryParser(in);
        job = parser.parse();
    }

    /**
     * 获取Job的详细信息
     *
     * @return
     */
    public String getJobDetail() {
        StringBuffer jobDetails = new StringBuffer("");
        SummarizedJob ts = new SummarizedJob(job);
        jobDetails.append(job.getJobId()).append("\t");
        jobDetails.append(job.getUsername()).append("\t");
        jobDetails.append(job.getJobname()).append("\t");
        jobDetails.append(job.getJobQueueName()).append("\t");
        jobDetails.append(job.getPriority()).append("\t");
        jobDetails.append(job.getJobConfPath()).append("\t");
        jobDetails.append(job.getUberized()).append("\t");
        jobDetails.append(
                StringUtils.getFormattedTimeWithDiff(dateFormat,
                        job.getSubmitTime(), 0)).append("\t");
        jobDetails.append(
                StringUtils.getFormattedTimeWithDiff(dateFormat,
                        job.getLaunchTime(), 0)).append("\t");
        jobDetails.append(
                StringUtils.getFormattedTimeWithDiff(dateFormat,
                        job.getFinishTime(), 0)).append("\t");
        jobDetails.append(
                ((job.getJobStatus() == null) ? "Incomplete" : job
                        .getJobStatus())).append("\t");
        jobDetails.append(ts.getTotalSetups()).append("\t");
        jobDetails.append(ts.getNumFinishedSetups()).append("\t");
        jobDetails.append(ts.getNumFailedSetups()).append("\t");
        jobDetails.append(ts.getNumKilledSetups()).append("\t");
        jobDetails.append(
                StringUtils.getFormattedTimeWithDiff(dateFormat,
                        ts.getSetupStarted(), 0)).append("\t");
        jobDetails.append(
                StringUtils.getFormattedTimeWithDiff(dateFormat,
                        ts.getSetupFinished(), 0)).append("\t");
        jobDetails.append(ts.getTotalMaps()).append("\t");
        jobDetails.append(ts.getTotalMaps() - ts.getNumFailedMaps()).append(
                "\t");
        jobDetails.append(ts.getNumFailedMaps()).append("\t");
        jobDetails.append(ts.getNumKilledMaps()).append("\t");
        jobDetails.append(
                StringUtils.getFormattedTimeWithDiff(dateFormat,
                        ts.getMapStarted(), 0)).append("\t");
        jobDetails.append(
                StringUtils.getFormattedTimeWithDiff(dateFormat,
                        ts.getMapFinished(), 0)).append("\t");

        jobDetails.append(ts.getTotalReduces()).append("\t");
        jobDetails.append(ts.getTotalReduces() - ts.getNumFailedReduces())
                .append("\t");
        jobDetails.append(ts.getNumFailedReduces()).append("\t");
        jobDetails.append(ts.getNumKilledReduces()).append("\t");
        jobDetails.append(
                StringUtils.getFormattedTimeWithDiff(dateFormat,
                        ts.getReduceStarted(), 0)).append("\t");
        jobDetails.append(
                StringUtils.getFormattedTimeWithDiff(dateFormat,
                        ts.getReduceFinished(), 0)).append("\t");

        jobDetails.append(ts.getTotalCleanups()).append("\t");
        jobDetails.append(ts.getNumFinishedCleanups()).append("\t");
        jobDetails.append(ts.getNumFailedCleanups()).append("\t");
        jobDetails.append(ts.getNumKilledCleanups()).append("\t");

        jobDetails.append(
                StringUtils.getFormattedTimeWithDiff(dateFormat,
                        ts.getCleanupStarted(), 0)).append("\t");
        jobDetails.append(
                StringUtils.getFormattedTimeWithDiff(dateFormat,
                        ts.getCleanupFinished(), 0)).append("\t");
        jobDetails.append(getJobCounters()).append("\t");
        jobDetails.append("\n");
        return jobDetails.toString();
    }

    /**
     * 获取Job的Counter 信息
     *
     * @return
     */
    public String getJobCounters() {
        StringBuffer jobCounter = new StringBuffer();
        if (job.getTotalCounters() != null) {
            Counters totalCounters = job.getTotalCounters();
            Counters mapCounters = job.getMapCounters();
            Counters reduceCounters = job.getReduceCounters();
            for (String groupName : totalCounters.getGroupNames()) {

                if (!groupName.equals("org.apache.hadoop.mapred.Task$Counter") && !groupName.equals("FileSystemCounters")) {
                    CounterGroup totalGroup = totalCounters.getGroup(groupName);
                    CounterGroup mapGroup = mapCounters.getGroup(groupName);
                    CounterGroup reduceGroup = reduceCounters.getGroup(groupName);
                    Iterator<Counter> ctrItr = totalGroup.iterator();
                    while (ctrItr.hasNext()) {
                        Counter counter = ctrItr.next();
                        String name = counter.getName();
                        Counter mapCounter = mapGroup.findCounter(name);
                        Counter reduceCounter = reduceGroup.findCounter(name);
                        if (FILE_BYTES_READ.equals(name)) {
                            jobCounter.append(mapCounter.getValue()).append("\t");
                            jobCounter.append(reduceCounter.getValue()).append("\t");
                        } else if (FILE_BYTES_WRITTEN.equals(name)) {
                            jobCounter.append(mapCounter.getValue()).append("\t");
                            jobCounter.append(reduceCounter.getValue()).append("\t");
                        } else if (HDFS_BYTES_READ.equals(name)) {
                            jobCounter.append(mapCounter.getValue()).append("\t");
                            jobCounter.append(reduceCounter.getValue()).append("\t");
                        } else if (HDFS_BYTES_WRITTEN.equals(name)) {
                            jobCounter.append(mapCounter.getValue()).append("\t");
                            jobCounter.append(reduceCounter.getValue()).append("\t");
                        } else if (HDFS_READ_OPS.equals(name)) {
                            jobCounter.append(mapCounter.getValue()).append("\t");
                            jobCounter.append(reduceCounter.getValue()).append("\t");
                        } else if (HDFS_WRITE_OPS.equals(name)) {
                            jobCounter.append(mapCounter.getValue()).append("\t");
                            jobCounter.append(reduceCounter.getValue()).append("\t");
                        } else if (GC_TIME_MILLIS.equals(name)) {
                            jobCounter.append(mapCounter.getValue()).append("\t");
                            jobCounter.append(reduceCounter.getValue()).append("\t");
                        } else if (CPU_TIME_SPENT.equals(name)) {
                            jobCounter.append(mapCounter.getValue()).append("\t");
                            jobCounter.append(reduceCounter.getValue()).append("\t");
                        } else if (PHYSICAL_MEMORY_BYTES.equals(name)) {
                            jobCounter.append(mapCounter.getValue()).append("\t");
                            jobCounter.append(reduceCounter.getValue()).append("\t");
                        }  else if (VIRTUAL_MEMORY_BYTES.equals(name)) {
                            jobCounter.append(mapCounter.getValue()).append("\t");
                            jobCounter.append(reduceCounter.getValue()).append("\t");
                        }else if (COMMITTED_HEAP_BYTES.equals(name)) {
                            jobCounter.append(mapCounter.getValue()).append("\t");
                            jobCounter.append(reduceCounter.getValue()).append("\t");
                        }
                    }
                }
            }
        } else {
            for (int i = 0; i < 22; i++) {
                jobCounter.append(NULL).append("\t");
            }
        }
        return jobCounter.toString();
    }


    /**
     * 获取Task的详细信息
     *
     * @param jobInfo
     * @return
     */
    public static String getTaskDetails(JobInfo jobInfo) {
        Map<TaskID, JobHistoryParser.TaskInfo> tasks = jobInfo.getAllTasks();
        StringBuffer taskDetails = new StringBuffer();
        for (JobHistoryParser.TaskInfo task : tasks.values()) {
            taskDetails.append(jobInfo.getJobId()).append("\t");
            taskDetails.append(task.getTaskId()).append("\t");
            taskDetails.append(task.getTaskType()).append("\t");
            taskDetails.append(
                    StringUtils.getFormattedTimeWithDiff(dateFormat,
                            task.getStartTime(), 0)).append("\t");
            taskDetails.append(
                    StringUtils.getFormattedTimeWithDiff(dateFormat,
                            task.getFinishTime(), task.getStartTime())).append(
                    "\t");
            taskDetails.append(task.getSplitLocations()).append("\t");
            taskDetails.append(task.getTaskStatus()).append("\t");
            taskDetails.append(getTaskCounter(task)).append("\t");
            taskDetails.append(task.getError()).append("\t");
            taskDetails.append("\n");
        }
        return taskDetails.toString();
    }

    /**
     * 获取Task 的Counter值
     *
     * @param taskInfo
     * @return
     */
    public static String getTaskCounter(JobHistoryParser.TaskInfo taskInfo) {
        StringBuffer taskCounter = new StringBuffer();
        Counters counters = taskInfo.getCounters();
        if (counters != null) {
            for (String groupName : counters.getGroupNames()) {
                CounterGroup taskGroup = counters.getGroup(groupName);
                Iterator<Counter> ctrItr = taskGroup.iterator();
                while (ctrItr.hasNext()) {
                    Counter counter = ctrItr.next();
                    String name = taskGroup.getName();
                    if (CPU_TIME_SPENT.equals(name)) {
                        taskCounter.append(counter.getValue()).append("\t");
                    } else if (GC_TIME_MILLIS.equals(name)) {
                        taskCounter.append(counter.getValue()).append("\t");
                    } else if (FILE_BYTES_READ.equals(name)) {
                        taskCounter.append(counter.getValue()).append("\t");
                    } else if (FILE_BYTES_WRITTEN.equals(name)) {
                        taskCounter.append(counter.getValue()).append("\t");
                    } else if (HDFS_BYTES_READ.equals(name)) {
                        taskCounter.append(counter.getValue()).append("\t");
                    } else if (HDFS_BYTES_WRITTEN.equals(name)) {
                        taskCounter.append(counter.getValue()).append("\t");
                    } else if (HDFS_READ_OPS.equals(name)) {
                        taskCounter.append(counter.getValue()).append("\t");
                    } else if (HDFS_WRITE_OPS.equals(name)) {
                        taskCounter.append(counter.getValue()).append("\t");
                    } else if (PHYSICAL_MEMORY_BYTES.equals(name)) {
                        taskCounter.append(counter.getValue()).append("\t");
                    } else if (COMMITTED_HEAP_BYTES.equals(name)) {
                        taskCounter.append(counter.getValue()).append("\t");
                    } else if (VIRTUAL_MEMORY_BYTES.equals(name)) {
                        taskCounter.append(counter.getValue()).append("\t");
                    }
                }
            }
        } else {
            for (int i = 0; i < 11; i++) {
                taskCounter.append(NULL).append("\t");
            }
        }
        return taskCounter.toString();
    }

}
