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


import java.io.IOException;
import java.net.ServerSocket;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class PutIgniteRecordTest {
    public static int getAvailablePort() throws IOException {
        try(ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

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
        String flowFileContent = "John;Doe;42;\n";
        PreparedStatement query = conn.prepareStatement("SELECT * FROM person WHERE first_name = 'John' AND last_name = 'Doe'");

        ResultSet resultBeforeRun = query.executeQuery();
        assertFalse(resultBeforeRun.next());

        runner.enqueue(flowFileContent, flowFileAttributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(success, 1);

        ResultSet resultAfterRun = query.executeQuery();
        assert(resultAfterRun.next());
        assertEquals(resultAfterRun.getString("first_name"), "John");
        assertEquals(resultAfterRun.getInt("age"), 42);
    }

    private void setupIgnite() throws IOException, SQLException {
        int ignitePort = getAvailablePort();
        int clientConnectorPort = getAvailablePort();
        ClientConnectorConfiguration clientConfiguration = new ClientConnectorConfiguration().setPort(clientConnectorPort);
        igniteServer = IgniteTestUtil.startServer(ignitePort, clientConfiguration);

        conn = DriverManager.getConnection("jdbc:ignite:thin://localhost:" + clientConnectorPort);
        conn.prepareStatement("CREATE TABLE person (first_name VARCHAR, last_name VARCHAR, age INT, PRIMARY KEY (first_name, last_name)) WITH \"CACHE_NAME=person,KEY_TYPE=person_key,VALUE_TYPE=person\"").execute();
    }
}
