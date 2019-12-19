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
import com.marklogic.mock.processor.EmployeeItemProcessor;
import com.marklogic.spring.batch.item.processor.MarkLogicItemProcessor;
import com.marklogic.spring.batch.item.writer.MarkLogicItemWriter;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.oxm.xstream.XStreamMarshaller;


import javax.sql.DataSource;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
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

   /* @Bean
    public EmployeeItemProcessor processor(){
        return new EmployeeItemProcessor();
    }

   @Bean
    ItemWriter<Employee> databaseXmlItemWriter(Environment environment) {
        StaxEventItemWriter<Employee> xmlFileWriter = new StaxEventItemWriter<>();

        String exportFilePath = environment.getRequiredProperty(PROPERTY_XML_EXPORT_FILE_PATH);
        xmlFileWriter.setResource(new FileSystemResource(exportFilePath));

        xmlFileWriter.setRootTagName("Employees");

        Jaxb2Marshaller studentMarshaller = new Jaxb2Marshaller();
        studentMarshaller.setClassesToBeBound(Employee.class);
        xmlFileWriter.setMarshaller(studentMarshaller);

        return xmlFileWriter;
    }

    @Bean
   public StaxEventItemWriter<Employee> writer(){
        StaxEventItemWriter<Employee> writer = new StaxEventItemWriter<Employee>();
        writer.setResource(new ClassPathResource("persons.xml"));

        Map<String,String> aliasesMap =new HashMap<String,String>();
        aliasesMap.put("employee", "com.marklogic.mock.model.Employee");
        XStreamMarshaller marshaller = new XStreamMarshaller();
        try {
            marshaller.setAliases(aliasesMap);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        writer.setMarshaller(marshaller);
        writer.setRootTagName("Employees");
        writer.setOverwriteOutput(true);
        return writer;
    }

   @Bean
    public Step step1(){
        return stepBuilderFactory.get("step1").<Employee,Employee>chunk(100).reader(reader()).processor(processor()).writer(writer()).build();
    }

    @Bean
    public Job exportPerosnJob(){
        return jobBuilderFactory.get("exportPeronJob").incrementer(new RunIdIncrementer()).flow(step1()).end().build();
    }
   @Bean
   ItemProcessor<Employee, Employee> databaseXmlItemProcessor() {
       return new EmployeeItemProcessor();
   }
   @Bean
   Step databaseToXmlFileStep(ItemReader<Employee> jdbcCursorItemReader,
                              ItemProcessor<Employee, Employee> databaseXmlItemProcessor,
                              ItemWriter<Employee> databaseXmlItemWriter,
                              StepBuilderFactory stepBuilderFactory) {
       return stepBuilderFactory.get("databaseToXmlFileStep")
               .<Employee, Employee>chunk(1)
               .reader(jdbcCursorItemReader)
               .processor(databaseXmlItemProcessor)
               .writer(databaseXmlItemWriter)
               .build();
   }

    @Bean
    Job databaseToXmlFileJob(JobBuilderFactory jobBuilderFactory,
                             @Qualifier("databaseToXmlFileStep") Step csvStudentStep) {
        return jobBuilderFactory.get("databaseToXmlFileJob")
                .incrementer(new RunIdIncrementer())
                .flow(csvStudentStep)
                .end()
                .build();
    }*/
   @Bean
   public Job job(JobBuilderFactory jobBuilderFactory, Step step) {


       return jobBuilderFactory.get("toMarklogicJob")
               .start(step)
               .incrementer(new RunIdIncrementer())
               .build();
   }
}

