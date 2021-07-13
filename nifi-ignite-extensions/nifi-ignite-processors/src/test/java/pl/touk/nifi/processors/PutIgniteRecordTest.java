package pl.touk.nifi.processors;

import org.apache.ignite.Ignite;
import org.apache.nifi.csv.CSVReader;
import org.apache.nifi.csv.CSVUtils;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import pl.touk.nifi.ignite.testutil.IgniteTestUtil;
import pl.touk.nifi.ignite.testutil.PortFinder;

import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PutIgniteRecordTest {

    private final Relationship success = new Relationship.Builder()
            .name("success")
            .build();

    private final String personCsvSchema =
            "{ \"type\": \"record\", \"name\": \"person\", \"fields\": " +
                    "[ { \"name\": \"first_name\", \"type\": \"string\" }, " +
                      "{ \"name\": \"last_name\", \"type\": \"string\" }, " +
                      "{ \"name\": \"birthday\", \"type\": {\n" +
                      "    \"type\": \"long\",\n" +
                      "    \"logicalType\": \"timestamp-millis\"\n" +
                      "  }\n " +
                      "}, " +
                      "{ \"name\": \"age\", \"type\": \"int\" } ] }";

    private static int ignitePort;
    private static Ignite igniteServer;
    private static int clientConnectorPort;

    private Connection conn;

    private TestRunner runner;
    private CSVReader csvReader;
    private Map<String, String> flowFileAttributes = new HashMap<>();

    private PreparedStatement queryAll;
    private PreparedStatement queryJohn;
    private PreparedStatement queryJane;

    @Before
    public void before() throws InitializationException, IOException, SQLException {
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

        conn = DriverManager.getConnection("jdbc:ignite:thin://localhost:" + clientConnectorPort);
        conn.prepareStatement("CREATE TABLE IF NOT EXISTS person (first_name VARCHAR, last_name VARCHAR, birthday TIMESTAMP, age INT, PRIMARY KEY (first_name, last_name)) WITH \"CACHE_NAME=person,KEY_TYPE=person_key,VALUE_TYPE=person\"").execute();

        queryAll = conn.prepareStatement("SELECT * FROM person");
        queryJohn = conn.prepareStatement("SELECT * FROM person WHERE first_name = 'John' AND last_name = 'Doe'");
        queryJane = conn.prepareStatement("SELECT * FROM person WHERE first_name = 'Jane' AND last_name = 'Doe'");
    }

    @After
    public void after() throws SQLException {
        conn.prepareStatement("DELETE FROM person").execute();
        conn.close();
        igniteServer.close();
    }

    @Test
    public void testProcessor() throws SQLException {
        ResultSet resultBeforeRun = queryAll.executeQuery();
        assertFalse(resultBeforeRun.next());

        String flowFileContent = "John;Doe;360720000;42\nJane;Doe;802483200;35\n";
        runner.enqueue(flowFileContent, flowFileAttributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(success, 1);

        Person john = selectPerson(queryJohn);
        assertEquals(john.firstName, "John");
        assertEquals(john.birthday, Timestamp.from(Instant.ofEpochMilli(360720000)));
        assertEquals(john.age, 42);

        Person jane = selectPerson(queryJane);
        assertEquals(jane.firstName, "Jane");
        assertEquals(jane.birthday, Timestamp.from(Instant.ofEpochMilli(802483200)));
        assertEquals(jane.age, 35);

        // When override is disabled it should not override cache entry
        runner.setProperty(PutIgniteRecord.DATA_STREAMER_ALLOW_OVERRIDE, "false");
        String johnAge43 = "John;Doe;329184000;43;\n";
        runner.enqueue(johnAge43, flowFileAttributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(success, 2);

        Person sameJohn = selectPerson(queryJohn);
        assertEquals(sameJohn.age, 42);

        // When override is enabled it should override cache entry
        runner.setProperty(PutIgniteRecord.DATA_STREAMER_ALLOW_OVERRIDE, "true");
        runner.enqueue(johnAge43, flowFileAttributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(success, 3);

        Person updatedJohn = selectPerson(queryJohn);
        assertEquals(updatedJohn.age, 43);
    }

    @BeforeClass
    public static void setupIgnite() throws IOException {
        ignitePort = PortFinder.getAvailablePort();
        clientConnectorPort = PortFinder.getAvailablePort();

        igniteServer = IgniteTestUtil.startServer(ignitePort, clientConnectorPort);
    }

    private Person selectPerson(PreparedStatement query) throws SQLException {
        ResultSet resultSet = query.executeQuery();
        assert(resultSet.next());
        return new Person(
                resultSet.getString("first_name"), resultSet.getString("last_name"),
                resultSet.getTimestamp("birthday"), resultSet.getInt("age")
        );
    }

    static class Person {
        private final String firstName;
        private final String lastName;
        private final Timestamp birthday;
        private final int age;

        Person(String firstName, String lastName, Timestamp birthday, int age) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.birthday = birthday;
            this.age = age;
        }
    }
}
