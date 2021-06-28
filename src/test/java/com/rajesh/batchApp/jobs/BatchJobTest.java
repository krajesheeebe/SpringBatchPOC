package com.rajesh.batchApp.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@SpringBootTest
public class BatchJobTest {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job storeCsvDataJob;

    @Test
    public void testBatchJob() throws Exception {

        JobParametersBuilder builder = new JobParametersBuilder();
        JobParameters parameters = builder
        		.addString("timestamp", new Date().toString())
                .addString("filename", "account-details.csv")
                .addString("tableName", "ACCOUNTDETAILS")
                .addString("scriptFilename", "create_table.sql")
                .toJobParameters();
        JobExecution execution = jobLauncher.run(storeCsvDataJob, parameters);

        List<StepExecution> stepExecutions = new ArrayList<>(execution.getStepExecutions());
        assertEquals(1, stepExecutions.size());
        StepExecution stepExecution = stepExecutions.get(0);
        assertEquals(ExitStatus.FAILED.getExitCode(), execution.getExitStatus().getExitCode());
        assertEquals(ExitStatus.FAILED.getExitCode(), stepExecution.getExitStatus().getExitCode());
        assertEquals(18, stepExecution.getReadCount());
        assertEquals(10, stepExecution.getWriteCount());

    }
}
