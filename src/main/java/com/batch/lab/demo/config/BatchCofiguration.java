package com.batch.lab.demo.config;

import com.batch.lab.demo.model.Employee;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@EnableScheduling
public class BatchCofiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Value("${file.input}")
    private String fileInput;

    @Bean
    public FlatFileItemReader<Employee> reader(){
        return new FlatFileItemReaderBuilder<Employee>().name("employeeItemReader").resource(new ClassPathResource(fileInput)).delimited()
                .names(new String[]{"employeeName","employeeId","employeeRole","contact","location"}).fieldSetMapper(new BeanWrapperFieldSetMapper<Employee>(){{setTargetType(Employee.class);}}).build();
    }

    @Bean
    public JdbcBatchItemWriter writer(DataSource dataSource){
        return new JdbcBatchItemWriterBuilder<Employee>().itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Employee>()).beanMapped()
                .sql("insert into employee(employeeName,employeeId,employeeRole,contactNumber,location) values (:employeeName, :employeeId, :employeeRole, :contact, :location)").dataSource(dataSource).build();

    }

    @Bean
    public Step loadStep(JobRepository jobRepository, DataSource dataSource,PlatformTransactionManager transactionManager){
        return stepBuilderFactory.get("parseEmployee").<Employee,Employee>chunk(10).reader(reader()).processor(new ItemProcessor<Employee, Employee>() {
            @Override
            public Employee process(Employee employee) throws Exception {
                return employee;
            }
        }).writer(writer(dataSource)).build();
    }

    @Bean
    public Job createEmployeeJob(Step loadStep){
        return jobBuilderFactory.get("employeeJob").incrementer(new RunIdIncrementer()).listener(new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                System.out.println("createEmployeeJob has started."+jobExecution.getStartTime());
            }

            @Override
            public void afterJob(JobExecution jobExecution) {

            }
        }).flow(loadStep).end().build();
    }



}
