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
import pl.touk.nifi.services.IgniteBinaryKeyBinaryObjectStreamer;
import pl.touk.nifi.services.IgniteThickClient;

import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class PutIgniteRecordTestV2 {

    private final Relationship success = new Relationship.Builder()
            .name("success")
            .build();

    private final String myRowCsvSchema =
            "{ \"type\": \"record\", \"name\": \"myRow\", \"fields\": " +
                    "[ { \"name\": \"COLUMNONE\", \"type\": \"long\" }, " +
                    "{ \"name\": \"columnTwo\", \"type\": \"long\" }, " +
                    "{ \"name\": \"columnThree\", \"type\": {\n" +
                    "    \"type\": \"long\",\n" +
                    "    \"logicalType\": \"timestamp-millis\"\n" +
                    "  }\n " +
                    "} ] } ";

    private static int ignitePort;
    private static Ignite igniteServer;
    private static int clientConnectorPort;

    private Connection conn;

    private TestRunner runner;
    private CSVReader csvReaderService;
    private IgniteThickClient thickClientService;
    private IgniteBinaryKeyBinaryObjectStreamer dataStreamerService;
    private Map<String, String> flowFileAttributes = new HashMap<>();

    private PreparedStatement queryAll;


    private PreparedStatement queryRow(Long COLUMNONE) throws SQLException {
        PreparedStatement st =  conn.prepareStatement("SELECT * FROM MY_TABLE WHERE \"COLUMNONE\" = ?");
        st.setLong(1, COLUMNONE);
        return st;
    }

    @Before
    public void before() throws InitializationException, IOException, SQLException {
        conn = DriverManager.getConnection("jdbc:ignite:thin://localhost:" + clientConnectorPort);
        conn.prepareStatement("CREATE TABLE MY_TABLE (\"COLUMNONE\" BIGINT,\"columnTwo\" BIGINT,\"columnThree\" TIMESTAMP,PRIMARY KEY (\"COLUMNONE\",\"columnThree\")) WITH \"CACHE_NAME=MY_TABLE,KEY_TYPE=MY_TABLE_KEY,VALUE_TYPE=MY_TABLE_VALUE\"").execute();

        queryAll = conn.prepareStatement("SELECT * FROM MY_TABLE");

        flowFileAttributes.put("csv.delimiter", ";");
        flowFileAttributes.put("csv.schema", myRowCsvSchema);

        csvReaderService = new CSVReader();
        thickClientService = new IgniteThickClient();
        dataStreamerService = new IgniteBinaryKeyBinaryObjectStreamer();

        runner = TestRunners.newTestRunner(PutIgniteRecord.class);

        runner.addControllerService("csv-reader-service", csvReaderService);
        runner.setProperty(csvReaderService, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY.getName(), SchemaAccessUtils.SCHEMA_TEXT_PROPERTY.getValue());
        runner.setProperty(csvReaderService, CSVUtils.VALUE_SEPARATOR.getName(), ";");
        runner.setProperty(csvReaderService, CSVReader.CSV_PARSER.getName(), CSVReader.CSV_PARSER.getDefaultValue());
        runner.setProperty(csvReaderService, CSVUtils.FIRST_LINE_IS_HEADER.getName(), "false");
        runner.setProperty(csvReaderService, SchemaAccessUtils.SCHEMA_TEXT.getName(), "${csv.schema}");
        runner.enableControllerService(csvReaderService);

        runner.addControllerService("thick-client-service", thickClientService);
        runner.enableControllerService(thickClientService);

        runner.addControllerService("data-streamer-service", dataStreamerService);
        runner.setProperty(dataStreamerService, IgniteBinaryKeyBinaryObjectStreamer.IGNITE_THICK_CLIENT_SERVICE.getName(), "thick-client-service");
        runner.setProperty(dataStreamerService, IgniteBinaryKeyBinaryObjectStreamer.CACHE_NAME.getName(), "MY_TABLE");
        runner.setProperty(dataStreamerService, IgniteBinaryKeyBinaryObjectStreamer.CACHE_KEY_TYPE.getName(), "MY_TABLE_KEY");
        runner.setProperty(dataStreamerService, IgniteBinaryKeyBinaryObjectStreamer.CACHE_VALUE_TYPE.getName(), "MY_TABLE_VALUE");
        runner.setProperty(dataStreamerService, IgniteBinaryKeyBinaryObjectStreamer.KEY_FIELD_NAMES.getName(), "COLUMNONE,columnThree");
        runner.setProperty(dataStreamerService, IgniteBinaryKeyBinaryObjectStreamer.DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS.getName(), "1");
        runner.setProperty(dataStreamerService, IgniteBinaryKeyBinaryObjectStreamer.DATA_STREAMER_AUTO_FLUSH_FREQUENCY.getName(), "0");
        runner.setProperty(dataStreamerService, IgniteBinaryKeyBinaryObjectStreamer.DATA_STREAMER_ALLOW_OVERRIDE.getName(), "false");
        runner.enableControllerService(dataStreamerService);

        runner.setProperty(PutIgniteRecord.RECORD_READER.getName(), "csv-reader-service");
        runner.setProperty(PutIgniteRecord.DATA_STREAMER_SERVICE.getName(), "data-streamer-service");
    }

    @After
    public void after() throws SQLException {
        if (conn != null) {
            conn.prepareStatement("DELETE FROM MY_TABLE").execute();
            conn.close();
        }
        if (igniteServer != null) {
            igniteServer.close();
        }
    }

    @Test
    public void testProcessor() throws SQLException {
        ResultSet resultBeforeRun = queryAll.executeQuery();
        assertFalse(resultBeforeRun.next());

        String flowFileContent = "1;1;360720000\n2;2;802483200\n";
        runner.enqueue(flowFileContent, flowFileAttributes);
        runner.run(1);
        runner.assertAllFlowFilesTransferred(success, 1);

        MyRow row1 = selectRow(queryRow(1L));
        assertEquals(new Long(1), row1.COLUMNONE);
        assertEquals(Timestamp.from(Instant.ofEpochMilli(360720000)), row1.columnThree);

        MyRow row2 = selectRow(queryRow(2L));
        assertEquals(new Long(2), row2.COLUMNONE);
        assertEquals(Timestamp.from(Instant.ofEpochMilli(802483200)), row2.columnThree);
    }

    @BeforeClass
    public static void setupIgnite() throws IOException {
        ignitePort = PortFinder.getAvailablePort();
        clientConnectorPort = PortFinder.getAvailablePort();

        igniteServer = IgniteTestUtil.startServer(ignitePort, clientConnectorPort);
    }

    private MyRow selectRow(PreparedStatement query) throws SQLException {
        ResultSet resultSet = query.executeQuery();
        assert(resultSet.next());
        return new MyRow(
                resultSet.getLong("COLUMNONE"), resultSet.getLong("columnTwo"), resultSet.getTimestamp("columnThree")
        );
    }

    static class MyRow {
        private final Long COLUMNONE;
        private final Long columnTwo;
        private final Timestamp columnThree;

        MyRow(Long COLUMNONE, Long columnTwo, Timestamp columnThree) {
            this.COLUMNONE = COLUMNONE;
            this.columnTwo = columnTwo;
            this.columnThree = columnThree;
        }
    }

}
