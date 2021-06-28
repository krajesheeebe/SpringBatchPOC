package com.rajesh.batchApp.config;

import java.beans.PropertyEditor;
import java.beans.PropertyEditorSupport;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.rajesh.batchApp.dto.AccountDetails;
import com.rajesh.batchApp.jobs.CSVDataProcessor;
import com.rajesh.batchApp.jobs.CSVDataSkipListener;
import com.rajesh.batchApp.jobs.ExecuteScriptTasklet;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	private CSVDataProcessor csvDataProcessor;
	
    @Autowired
    private DataSource dataSource;

    @Autowired
    private ExecuteScriptTasklet executeScriptTasklet;

	@Bean
	Job storeCsvDataJob(Step executeScriptStep, Step storeCsvDataStep) {
		return jobBuilderFactory.get("storeCsvDataJob").flow(executeScriptStep).next(storeCsvDataStep).end().build();
	}
	
    @Bean
    Step executeScriptStep() {
        return stepBuilderFactory.get("executeScriptTasklet")
                .tasklet(executeScriptTasklet)
                .build();
    }

	@Bean
	Step storeCsvDataStep(FlatFileItemReader<AccountDetails> csvFileReader, JdbcBatchItemWriter<AccountDetails> csvDataBatchWriter) {
		return stepBuilderFactory.get("storeCsvDataStep")
				.<AccountDetails, AccountDetails>chunk(100)
				.reader(csvFileReader)
				.processor(csvDataProcessor)
				.writer(csvDataBatchWriter)
				.faultTolerant()
				.skipLimit(10)		// maximum limit to skip undesired records before failing the job
				.skip(FlatFileParseException.class)		// skip the record in case of these exceptions
				.skip(IllegalArgumentException.class)
				.retryLimit(3)		// maximum times to retry before failing the job
				.retry(SQLException.class)		// retry the record in case of these exceptions
				.listener(new CSVDataSkipListener())		// listener to log in case of skipped records
				.build();
	}
	
    @Bean
    @StepScope
    FlatFileItemReader<AccountDetails> csvFileReader(@Value("#{jobParameters['filename']}") String filename) {
        String[] properties = new String[] {"movieId", "tagId", "relevance"};
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setIncludedFields(IntStream.range(0, properties.length).toArray());
        lineTokenizer.setNames(properties);
        BeanWrapperFieldSetMapper<AccountDetails> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setStrict(false);
        fieldSetMapper.setTargetType(AccountDetails.class);
//        to convert any field to different data type
//        fieldSetMapper.setCustomEditors(getCustomEditors());
        DefaultLineMapper<AccountDetails> lineMapper = new DefaultLineMapper<>();
        lineMapper.setFieldSetMapper(fieldSetMapper);
        lineMapper.setLineTokenizer(lineTokenizer);
        Resource resource = new ClassPathResource(filename);
        FlatFileItemReader<AccountDetails> reader = new FlatFileItemReader<>();
        reader.setLinesToSkip(1);
        reader.setLineMapper(lineMapper);
        reader.setResource(resource);
        return reader;
    }
    
	private Map<Class<?>, PropertyEditor> getCustomEditors() {
		Map<Class<?>, PropertyEditor> editors = new HashMap<>();
		editors.put(LocalDate.class, new PropertyEditorSupport() {
			@Override
			public void setAsText(String text) {
				super.setValue(LocalDate.parse(text));
			}
		});
		return editors;
	}
    
    @Bean
    @StepScope
    JdbcBatchItemWriter<AccountDetails> csvDataBatchWriter(@Value("#{jobParameters['tableName']}") String tableName) {
        String[] columns = {"movie_id", "tag_id", "relevance"};
        String[] properties = {"movieId", "tagId", "relevance"};
        return new JdbcBatchItemWriterBuilder<AccountDetails>()
                .beanMapped()
                .sql(buildInsertSql(tableName, columns, properties))
                .dataSource(dataSource)
                .build();
    }

    private static String buildInsertSql(String tableName,
            String[] columns,
            String[] properties) {
        return "INSERT INTO " + tableName + " (" +
                String.join(",", columns) +
                ") VALUES (" +
                String.join(",", Arrays.stream(properties).map(p -> ":" + p).toArray(String[]::new)) +
                ")";
    }

}
