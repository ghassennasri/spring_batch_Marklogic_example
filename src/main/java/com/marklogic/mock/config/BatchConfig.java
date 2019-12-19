package com.marklogic.mock.config;


import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.helper.DatabaseClientProvider;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.client.io.marker.DocumentMetadataWriteHandle;
import com.marklogic.mock.model.Employee;
import com.marklogic.spring.batch.item.processor.MarkLogicItemProcessor;
import com.marklogic.spring.batch.item.writer.MarkLogicItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.sql.DataSource;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.StringWriter;
import java.util.UUID;


@Configuration
@EnableBatchProcessing
//@PropertySource(value = "application.properties")
@Import(value = {
        com.marklogic.spring.batch.config.MarkLogicBatchConfiguration.class,
        com.marklogic.spring.batch.config.MarkLogicConfiguration.class})
public class BatchConfig {

    private static final String PROPERTY_XML_EXPORT_FILE_PATH = "database.to.xml.job.export.file.path";

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource;

    @Bean
    public DatabaseClientProvider getDatabaseClientProvider(){
        return()->{
            return DatabaseClientFactory.newClient("localhost", 8011,
                   new DatabaseClientFactory.DigestAuthContext("admin", "admin"));
        };
    }


    public JdbcCursorItemReader<Employee> getReader(){
        JdbcCursorItemReader<Employee> cursorItemReader = new JdbcCursorItemReader<>();
        cursorItemReader.setDataSource(dataSource);
        cursorItemReader.setSql("SELECT emp_no,first_name,last_name,gender,birth_date,hire_date FROM employees");
        cursorItemReader.setRowMapper(new EmployeeRowMapper());
        return cursorItemReader;
    }
    @Bean
    @JobScope
    public Step step(
            StepBuilderFactory stepBuilderFactory,
            DatabaseClientProvider databaseClientProvider/*,
            @Value("#{jobParameters['output_collections'] ?: 'yourJob'}") String[] collections,
            @Value("#{jobParameters['chunk_size'] ?: 20}") int chunkSize*/) {

        DatabaseClient databaseClient = databaseClientProvider.getDatabaseClient();

        JdbcCursorItemReader<Employee>reader=getReader();

        //The ItemProcessor is typically customized for your Job.  An anoymous class is a nice way to instantiate but
        //if it is a reusable component instantiate in its own class
        MarkLogicItemProcessor<Employee> processor = item -> {
            DocumentWriteOperation dwo = new DocumentWriteOperation() {

                @Override
                public OperationType getOperationType() {
                    return OperationType.DOCUMENT_WRITE;
                }

                @Override
                public String getUri() {
                    return UUID.randomUUID().toString() + ".xml";
                }

                @Override
                public DocumentMetadataWriteHandle getMetadata() {
                    DocumentMetadataHandle metadata = new DocumentMetadataHandle();
                    metadata.withCollections("mysqlImport");
                    return metadata;
                }

                @Override
                public AbstractWriteHandle getContent() {
                    JAXBContext jaxbContext = null;
                    String xmlContent="";
                    try {
                        jaxbContext = JAXBContext.newInstance(Employee.class);
                        Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
                        jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
                        StringWriter sw = new StringWriter();
                        jaxbMarshaller.marshal(item, sw);
                        xmlContent = sw.toString();
                    } catch (JAXBException e) {
                        e.printStackTrace();
                    }

                    return new StringHandle(xmlContent);
                }

                @Override
                public String getTemporalDocumentURI() {
                    return null;
                }
            };
            return dwo;
        };

        MarkLogicItemWriter writer = new MarkLogicItemWriter(databaseClient);
        writer.setBatchSize(10000);




        return stepBuilderFactory.get("step1")
                .<Employee, DocumentWriteOperation>chunk(10000)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }


   @Bean
   public Job job(JobBuilderFactory jobBuilderFactory, Step step) {


       return jobBuilderFactory.get("toMarklogicJob")
               .start(step)
               .incrementer(new RunIdIncrementer())
               .build();
   }
}

