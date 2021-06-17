package pl.touk.nifi.processors;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import pl.touk.nifi.ignite.testutil.IgniteTestUtil;
import pl.touk.nifi.ignite.testutil.PortFinder;


import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class PutIgniteRecordTest {

    private final Relationship success = new Relationship.Builder()
            .name("success")
            .build();

    private final String personCsvSchema =
            "{ \"type\": \"record\", \"name\": \"person\", \"fields\": " +
                    "[ { \"name\": \"first_name\", \"type\": \"string\" }, { \"name\": \"last_name\", \"type\": \"string\" }, { \"name\": \"age\", \"type\": \"int\" } ] }";

    private Ignite igniteServer;
    private Connection conn;
    private TestRunner runner;
    private CSVReader csvReader;
    private Map<String, String> flowFileAttributes = new HashMap<>();

    @Before
    public void before() throws InitializationException, IOException, SQLException {
        setupIgnite();

        flowFileAttributes.put("csv.delimiter", ";");
        flowFileAttributes.put("csv.schema", personCsvSchema);

        csvReader = new CSVReader();

        runner = TestRunners.newTestRunner(PutIgniteRecord.class);

        runner.addControllerService("csv-reader", csvReader);
        runner.setProperty(csvReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY.getName(), SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        runner.setProperty(csvReader, CSVUtils.VALUE_SEPARATOR.getName(), ";");
        runner.setProperty(csvReader, CSVReader.CSV_PARSER.getName(), CSVReader.CSV_PARSER.getDefaultValue());
        runner.setProperty(csvReader, CSVUtils.FIRST_LINE_IS_HEADER.getName(), "false");
        runner.setProperty(csvReader, SchemaAccessUtils.SCHEMA_TEXT.getName(), "${csv.schema}");
        runner.enableControllerService(csvReader);

        runner.setProperty(PutIgniteRecord.RECORD_READER.getName(), "csv-reader");
        runner.setProperty(PutIgniteRecord.CACHE_NAME.getName(), "person");
        runner.setProperty(PutIgniteRecord.CACHE_KEY_TYPE.getName(), "person_key");
        runner.setProperty(PutIgniteRecord.CACHE_VALUE_TYPE.getName(), "person");
        runner.setProperty(PutIgniteRecord.KEY_FIELD_NAMES.getName(), "first_name,last_name");
    }

    @After
    public void after() throws SQLException {
        conn.close();
        igniteServer.close();
    }

    @Test
    public void testProcessor() throws SQLException {
        PreparedStatement queryAll = conn.prepareStatement("SELECT * FROM person");
        PreparedStatement queryJohn = conn.prepareStatement("SELECT * FROM person WHERE first_name = 'John' AND last_name = 'Doe'");
        PreparedStatement queryJane = conn.prepareStatement("SELECT * FROM person WHERE first_name = 'Jane' AND last_name = 'Doe'");

        ResultSet resultBeforeRun = queryAll.executeQuery();
        assertFalse(resultBeforeRun.next());

        String flowFileContent = "John;Doe;42\nJane;Doe;35\n";
        runner.enqueue(flowFileContent, flowFileAttributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(success, 1);

        ResultSet queryJohnResult1 = queryJohn.executeQuery();
        assert(queryJohnResult1.next());
        assertEquals(queryJohnResult1.getString("first_name"), "John");
        assertEquals(queryJohnResult1.getInt("age"), 42);

        ResultSet queryJaneResult1 = queryJane.executeQuery();
        assert(queryJaneResult1.next());
        assertEquals(queryJaneResult1.getString("first_name"), "Jane");
        assertEquals(queryJaneResult1.getInt("age"), 35);

        // When override is disabled it should not override cache entry
        runner.setProperty(PutIgniteRecord.DATA_STREAMER_ALLOW_OVERRIDE, "false");
        String johnAge43 = "John;Doe;43;\n";
        runner.enqueue(johnAge43, flowFileAttributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(success, 2);
        ResultSet queryJohnResult2 = queryJohn.executeQuery();
        assert(queryJohnResult2.next());
        assertEquals(queryJohnResult2.getInt("age"), 42);

        // When override is enabled it should override cache entry
        runner.setProperty(PutIgniteRecord.DATA_STREAMER_ALLOW_OVERRIDE, "true");
        runner.enqueue(johnAge43, flowFileAttributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(success, 3);
        ResultSet queryJohnResult3 = queryJohn.executeQuery();
        assert(queryJohnResult3.next());
        assertEquals(queryJohnResult3.getInt("age"), 43);
    }

    private void setupIgnite() throws IOException, SQLException {
        int ignitePort = PortFinder.getAvailablePort();
        int clientConnectorPort = PortFinder.getAvailablePort();
        ClientConnectorConfiguration clientConfiguration = new ClientConnectorConfiguration().setPort(clientConnectorPort);
        igniteServer = IgniteTestUtil.startServer(ignitePort, clientConfiguration);

        conn = DriverManager.getConnection("jdbc:ignite:thin://localhost:" + clientConnectorPort);
        conn.prepareStatement("CREATE TABLE person (first_name VARCHAR, last_name VARCHAR, age INT, PRIMARY KEY (first_name, last_name)) WITH \"CACHE_NAME=person,KEY_TYPE=person_key,VALUE_TYPE=person\"").execute();
    }
}
