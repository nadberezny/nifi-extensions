package pl.touk.nifi.services;

import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import pl.touk.nifi.ignite.DataStreamerResult;
import pl.touk.nifi.ignite.IgniteRecordStreamerService;
import pl.touk.nifi.ignite.IgniteThickClientService;

import javax.cache.CacheException;
import java.util.List;
import java.util.Map;

public abstract class AbstractIgniteRecordStreamerService<K, V> extends AbstractControllerService implements IgniteRecordStreamerService {

    public static final PropertyDescriptor IGNITE_THICK_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("ignite-thick-client-service")
            .displayName("Ignite thick client service")
            .required(true)
            .identifiesControllerService(IgniteThickClientService.class)
            .build();

    public static final PropertyDescriptor CACHE_NAME = new PropertyDescriptor.Builder()
            .displayName("Ignite Cache Name")
            .name("ignite-cache-name")
            .description("The name of the ignite cache")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS = new PropertyDescriptor.Builder()
            .displayName("Data Streamer Per Node Parallel Operations")
            .name("data-streamer-per-node-parallel-operations")
            .description("Data streamer per node parallelism")
            .defaultValue("5")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(1, 10, true))
            .sensitive(false)
            .build();

    public static final PropertyDescriptor DATA_STREAMER_PER_NODE_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .displayName("Data Streamer Per Node Buffer Size")
            .name("data-streamer-per-node-buffer-size")
            .description("Data streamer per node buffer size (1-500).")
            .defaultValue("250")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(1, 500, true))
            .sensitive(false)
            .build();

    public static final PropertyDescriptor DATA_STREAMER_AUTO_FLUSH_FREQUENCY = new PropertyDescriptor.Builder()
            .displayName("Data Streamer Auto Flush Frequency in millis")
            .name("data-streamer-auto-flush-frequency-in-millis")
            .description("Data streamer flush interval in millis seconds. If 0 is set, data will be flushed on each flowfile")
            .defaultValue("10")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(0, 1000, true))
            .sensitive(false)
            .build();

    public static final PropertyDescriptor DATA_STREAMER_ALLOW_OVERRIDE = new PropertyDescriptor.Builder()
            .displayName("Data Streamer Allow Override")
            .name("data-streamer-allow-override")
            .description("Whether to override values already in the cache")
            .defaultValue("true")
            .required(true)
            .allowableValues(new AllowableValue("true"), new AllowableValue("false"))
            .sensitive(false)
            .build();

    protected ConfigurationContext configContext;

    protected IgniteThickClientService thickClient;

    protected IgniteDataStreamer<K, V> dataStreamer;

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        configContext = context;
        thickClient = context
                .getProperty(IGNITE_THICK_CLIENT_SERVICE)
                .asControllerService(IgniteThickClientService.class);
        initDataStreamer(context);
    }

    @OnDisabled
    public void onDisabled() {
        closeDataStreamer();
    }

    protected DataStreamerResult addEntries(List<Map.Entry<K, V>> entries) {
        DataStreamerResult streamerResult = new DataStreamerResult();
        try {
            if (dataStreamer == null) {
                initDataStreamer(configContext);
            }
            dataStreamer.addData(entries);
            if (dataStreamer.autoFlushFrequency() == 0) {
                dataStreamer.flush();
            }
        } catch (CacheException e) {
            streamerResult.setError(e);
            if (e.getCause() instanceof IgniteClientDisconnectedException) {
                dataStreamer = null;
            }
        }
        return streamerResult;
    }

    protected void initDataStreamer(ConfigurationContext context) {
        if ( dataStreamer != null ) {
            return;
        }
        String cacheName = context.getProperty(CACHE_NAME).getValue();
        int perNodeParallelOperations = context.getProperty(DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS).asInteger();
        int perNodeBufferSize = context.getProperty(DATA_STREAMER_PER_NODE_BUFFER_SIZE).asInteger();
        int autoFlushFrequency = context.getProperty(DATA_STREAMER_AUTO_FLUSH_FREQUENCY).asInteger();
        boolean allowOverride = context.getProperty(DATA_STREAMER_ALLOW_OVERRIDE).asBoolean();

        dataStreamer = thickClient.getIgnite().dataStreamer(cacheName);
        dataStreamer.perNodeBufferSize(perNodeBufferSize);
        dataStreamer.perNodeParallelOperations(perNodeParallelOperations);
        dataStreamer.autoFlushFrequency(autoFlushFrequency);
        dataStreamer.allowOverwrite(allowOverride);
    }

    protected void closeDataStreamer() {
        if (dataStreamer != null) {
            getLogger().info("Closing ignite data streamer");
            try {
                dataStreamer.flush();
            } catch (CacheException ex) {
                getLogger().info("Exception while closing Ignite DataStreamer", ex);
            } finally {
                dataStreamer = null;
            }
        }
    }
}
