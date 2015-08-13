package com.jd.bdp.hadoop.jobhistory;

import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Utility class used the summarize the job.
 * Used by HistoryViewer and the JobHistory UI.
 *
 */
public  class SummarizedJob {
    int totalMaps = 0;
    int totalReduces = 0;
    int totalCleanups = 0;
    int totalSetups = 0;
    int numFailedMaps = 0;
    int numKilledMaps = 0;
    int numFailedReduces = 0;
    int numKilledReduces = 0;
    int numFinishedCleanups = 0;
    int numFailedCleanups = 0;
    int numKilledCleanups = 0;
    int numFinishedSetups = 0;
    int numFailedSetups = 0;
    int numKilledSetups = 0;
    long mapStarted = 0;
    long mapFinished = 0;
    long reduceStarted = 0;
    long reduceFinished = 0;
    long cleanupStarted = 0;
    long cleanupFinished = 0;
    long setupStarted = 0;
    long setupFinished = 0;

    /** Get total maps */
    public int getTotalMaps() { return totalMaps; }
    /** Get total reduces */
    public int getTotalReduces() { return totalReduces; }
    /** Get number of clean up tasks */
    public int getTotalCleanups() { return totalCleanups; }
    /** Get number of set up tasks */
    public int getTotalSetups() { return totalSetups; }
    /** Get number of failed maps */
    public int getNumFailedMaps() { return numFailedMaps; }
    /** Get number of killed maps */
    public int getNumKilledMaps() { return numKilledMaps; }
    /** Get number of failed reduces */
    public int getNumFailedReduces() { return numFailedReduces; }
    /** Get number of killed reduces */
    public int getNumKilledReduces() { return numKilledReduces; }
    /** Get number of cleanup tasks that finished */
    public int getNumFinishedCleanups() { return numFinishedCleanups; }
    /** Get number of failed cleanup tasks */
    public int getNumFailedCleanups() { return numFailedCleanups; }
    /** Get number of killed cleanup tasks */
    public int getNumKilledCleanups() { return numKilledCleanups; }
    /** Get number of finished set up tasks */
    public int getNumFinishedSetups() { return numFinishedSetups; }
    /** Get number of failed set up tasks */
    public int getNumFailedSetups() { return numFailedSetups; }
    /** Get number of killed set up tasks */
    public int getNumKilledSetups() { return numKilledSetups; }
    /** Get number of maps that were started */
    public long getMapStarted() { return mapStarted; }
    /** Get number of maps that finished */
    public long getMapFinished() { return mapFinished; }
    /** Get number of Reducers that were started */
    public long getReduceStarted() { return reduceStarted; }
    /** Get number of reducers that finished */
    public long getReduceFinished() { return reduceFinished; }
    /** Get number of cleanup tasks started */
    public long getCleanupStarted() { return cleanupStarted; }
    /** Get number of cleanup tasks that finished */
    public long getCleanupFinished() { return cleanupFinished; }
    /** Get number of setup tasks that started */
    public long getSetupStarted() { return setupStarted; }
    /** Get number of setup tasks that finished */
    public long getSetupFinished() { return setupFinished; }

    /** Create summary information for the parsed job */
    public SummarizedJob(JobHistoryParser.JobInfo job) {
        Map<TaskID, JobHistoryParser.TaskInfo> tasks = job.getAllTasks();

        for (JobHistoryParser.TaskInfo task : tasks.values()) {
            Map<TaskAttemptID, JobHistoryParser.TaskAttemptInfo> attempts =
                    task.getAllTaskAttempts();
            //allHosts.put(task.getHo(Keys.HOSTNAME), "");
            for (JobHistoryParser.TaskAttemptInfo attempt : attempts.values()) {
                long startTime = attempt.getStartTime();
                long finishTime = attempt.getFinishTime();
                if (TaskType.MAP.equals(attempt.getTaskType())) {
                    if (mapStarted== 0 || mapStarted > startTime) {
                        mapStarted = startTime;
                    }
                    if (mapFinished < finishTime) {
                        mapFinished = finishTime;
                    }
                    totalMaps++;
                    if (TaskStatus.State.FAILED.toString().equals(attempt.getTaskStatus())) {
                        numFailedMaps++;
                    } else if (TaskStatus.State.KILLED.toString().equals
                            (attempt.getTaskStatus())) {
                        numKilledMaps++;
                    }
                } else if (TaskType.REDUCE.equals(attempt.getTaskType())) {
                    if (reduceStarted==0||reduceStarted > startTime) {
                        reduceStarted = startTime;
                    }
                    if (reduceFinished < finishTime) {
                        reduceFinished = finishTime;
                    }
                    totalReduces++;
                    if (TaskStatus.State.FAILED.toString().equals
                            (attempt.getTaskStatus())) {
                        numFailedReduces++;
                    } else if (TaskStatus.State.KILLED.toString().equals
                            (attempt.getTaskStatus())) {
                        numKilledReduces++;
                    }
                } else if (TaskType.JOB_CLEANUP.equals(attempt.getTaskType())) {
                    if (cleanupStarted==0||cleanupStarted > startTime) {
                        cleanupStarted = startTime;
                    }
                    if (cleanupFinished < finishTime) {
                        cleanupFinished = finishTime;
                    }
                    totalCleanups++;
                    if (TaskStatus.State.SUCCEEDED.toString().equals
                            (attempt.getTaskStatus())) {
                        numFinishedCleanups++;
                    } else if (attempt.getTaskStatus().equals
                            (TaskStatus.State.FAILED.toString())) {
                        numFailedCleanups++;
                    } else if (TaskStatus.State.KILLED.toString().equals
                            (attempt.getTaskStatus())) {
                        numKilledCleanups++;
                    }
                } else if (TaskType.JOB_SETUP.equals(attempt.getTaskType())) {
                    if (setupStarted==0||setupStarted > startTime) {
                        setupStarted = startTime;
                    }
                    if (setupFinished < finishTime) {
                        setupFinished = finishTime;
                    }
                    totalSetups++;
                    if (TaskStatus.State.SUCCEEDED.toString().equals
                            (attempt.getTaskStatus())) {
                        numFinishedSetups++;
                    } else if (TaskStatus.State.FAILED.toString().equals
                            (attempt.getTaskStatus())) {
                        numFailedSetups++;
                    } else if (TaskStatus.State.KILLED.toString().equals
                            (attempt.getTaskStatus())) {
                        numKilledSetups++;
                    }
                }
            }
        }
    }
}