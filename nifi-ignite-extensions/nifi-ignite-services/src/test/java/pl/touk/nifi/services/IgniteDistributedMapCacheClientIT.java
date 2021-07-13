package pl.touk.nifi.services;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import pl.touk.nifi.ignite.testutil.IgniteTestUtil;
import pl.touk.nifi.ignite.testutil.PortFinder;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class IgniteDistributedMapCacheClientIT {

    private final static String CACHE_NAME = "my-cache";

    private final Serializer<String> stringSerializer = new StringSerializer();
    private final Deserializer<String> stringDeserializer = new StringDeserializer();

    private Ignite igniteServer;
    private IgniteDistributedMapCacheClient service;

    @Before
    public void before() throws IOException, InitializationException {
        int ignitePort = PortFinder.getAvailablePort();
        int clientConnectorPort = PortFinder.getAvailablePort();
        igniteServer = IgniteTestUtil.startServer(ignitePort, clientConnectorPort);

        TestRunner runner = TestRunners.newTestRunner(TestDistributedMapCacheClientProcessor.class);
        service = new IgniteDistributedMapCacheClient();
        runner.addControllerService("ignite-distributed-map-cache-client", service);
        runner.setProperty(service, IgniteDistributedMapCacheClient.SERVER_ADDRESSES, "localhost:" + clientConnectorPort);
        runner.setProperty(service, IgniteDistributedMapCacheClient.CACHE_NAME, CACHE_NAME);
        runner.enableControllerService(service);
        runner.assertValid(service);
    }

    @After
    public void after() throws Exception {
        service.onDisabled();
        igniteServer.close();
    }

    @Test
    public void test() throws IOException {
        // #get #put
        Assert.assertNull(
                service.get("key1", stringSerializer, stringDeserializer));
        service.put("key1", "value1", stringSerializer, stringSerializer);
        Assert.assertEquals(
                "value1",
                service.get("key1", stringSerializer, stringDeserializer));

        // #getAndPutIfAbsent
        Assert.assertEquals(
                "value2",
                service.getAndPutIfAbsent("key2", "value2", stringSerializer, stringSerializer, stringDeserializer));
        Assert.assertNull(
                service.getAndPutIfAbsent("key2", "value2", stringSerializer, stringSerializer, stringDeserializer));

        // #containsKey
        Assert.assertFalse(
                service.containsKey("key3", stringSerializer));
        service.put("key3", "value3", stringSerializer, stringSerializer);
        Assert.assertTrue(
                service.containsKey("key3", stringSerializer));

        // #putIfAbsent
        Assert.assertTrue(
                service.putIfAbsent("key4", "value4", stringSerializer, stringSerializer));
        Assert.assertFalse(
                service.putIfAbsent("key4", "value4", stringSerializer, stringSerializer));

        // #remove
        service.put("key5", "value5", stringSerializer, stringSerializer);
        Assert.assertTrue(
                service.remove("key5", stringSerializer));
        Assert.assertFalse(
                service.remove("key5", stringSerializer));

        // #removeByPattern
        Assert.assertThrows(
                "Remove by pattern is not supported",
                UnsupportedOperationException.class,
                () -> service.removeByPattern(""));
    }

    private static class StringSerializer implements Serializer<String> {
        @Override
        public void serialize(String value, OutputStream output) throws SerializationException, IOException {
            if (value != null) {
                output.write(value.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private static class StringDeserializer implements Deserializer<String> {
        @Override
        public String deserialize(byte[] input) throws DeserializationException, IOException {
            return input == null ? null : new String(input, StandardCharsets.UTF_8);
        }
    }
}
