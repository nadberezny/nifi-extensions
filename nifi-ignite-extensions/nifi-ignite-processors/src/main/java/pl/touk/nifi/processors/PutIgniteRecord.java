package pl.touk.nifi.processors;

import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteDataStreamerTimeoutException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import pl.touk.nifi.ignite.IgniteBinaryBuilder;

import javax.cache.CacheException;
import java.util.*;

@EventDriven
@SupportsBatching
@Tags({ "Ignite", "insert", "update", "stream", "write", "put", "cache", "key", "record" })
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Puts records from FlowFile into Ignite Cache using DataStreamer.")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutIgniteRecord extends AbstractGenericIgniteCacheProcessor<BinaryObject, BinaryObject> {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .displayName("Record Reader")
            .name("record-reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
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
            .description("Data streamer flush interval in millis seconds")
            .defaultValue("10")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(1, 100, true))
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

    public static final PropertyDescriptor DATA_STREAMER_MAX_RETRIES_ON_FAILURE = new PropertyDescriptor.Builder()
            .displayName("Data Streamer max retries on failure")
            .name("data-streamer-max-retries-on-failure")
            .description("Data Streamer max retry count in case of failure")
            .defaultValue("5")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 100, true))
            .sensitive(false)
            .build();

    protected static final List<PropertyDescriptor> descriptors =
            Arrays.asList(IGNITE_CONFIGURATION_FILE,CACHE_NAME, CACHE_KEY_TYPE, CACHE_VALUE_TYPE,
                    RECORD_READER, KEY_FIELD_NAMES,
                    DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS,
                    DATA_STREAMER_PER_NODE_BUFFER_SIZE,
                    DATA_STREAMER_AUTO_FLUSH_FREQUENCY,
                    DATA_STREAMER_ALLOW_OVERRIDE,
                    DATA_STREAMER_MAX_RETRIES_ON_FAILURE);

    private transient IgniteDataStreamer<BinaryObject, BinaryObject> igniteDataStreamer;

    private Integer maxRetries;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnStopped
    public final void closeIgniteDataStreamer() {
        if (igniteDataStreamer != null) {
            getLogger().info("Closing ignite data streamer");
            try {
                igniteDataStreamer.flush();
            } catch (CacheException ex) {
                getLogger().info("Exception while closing Ignite DataStreamer", ex);
            } finally {
                igniteDataStreamer = null;
            }
        }
    }

    @OnShutdown
    public final void closeIgniteDataStreamerAndCache() {
        closeIgniteDataStreamer();
        super.closeIgniteCache();
    }

    protected IgniteDataStreamer<BinaryObject, BinaryObject> getIgniteDataStreamer() {
        return igniteDataStreamer;
    }

    @OnScheduled
    public final void initializeIgniteDataStreamer(ProcessContext context) throws ProcessException {
        getLogger().info("Initializing Ignite DataStreamer");

        super.initializeIgniteCache(context);

        if ( getIgniteDataStreamer() != null ) {
            return;
        }

        getLogger().info("Creating Ignite DataStreamer");
        try {
            int perNodeParallelOperations = context.getProperty(DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS).asInteger();
            int perNodeBufferSize = context.getProperty(DATA_STREAMER_PER_NODE_BUFFER_SIZE).asInteger();
            int autoFlushFrequency = context.getProperty(DATA_STREAMER_AUTO_FLUSH_FREQUENCY).asInteger();
            boolean allowOverride = context.getProperty(DATA_STREAMER_ALLOW_OVERRIDE).asBoolean();
            maxRetries = context.getProperty(DATA_STREAMER_MAX_RETRIES_ON_FAILURE).asInteger();

            igniteDataStreamer = getIgnite().dataStreamer(getIgniteCache().getName());
            igniteDataStreamer.perNodeBufferSize(perNodeBufferSize);
            igniteDataStreamer.perNodeParallelOperations(perNodeParallelOperations);
            igniteDataStreamer.autoFlushFrequency(autoFlushFrequency);
            igniteDataStreamer.allowOverwrite(allowOverride);

        } catch (Exception e) {
            getLogger().error("Failed to schedule PutIgniteRecord due to {}", new Object[] { e }, e);
            throw new ProcessException(e);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final String[] keyFieldNames = context.getProperty(KEY_FIELD_NAMES).getValue().split(",");
        final String cacheKeyType = context.getProperty(CACHE_KEY_TYPE).getValue();
        final String cacheValueType = context.getProperty(CACHE_VALUE_TYPE).getValue();
        final IgniteBinaryBuilder binaryBuilder = new IgniteBinaryBuilder(getIgnite().binary());

        List<Map.Entry<BinaryObject, BinaryObject>> cacheItems = new ArrayList<>();

        session.read(flowFile, in -> {
            try(final RecordReader reader = readerFactory.createRecordReader(flowFile.getAttributes(), in, flowFile.getSize(), getLogger())) {
                Record record;
                while ((record = reader.nextRecord()) != null) {
                    BinaryObject binaryKeyObject = binaryBuilder.fromMapRecord((MapRecord) record, cacheKeyType, keyFieldNames);
                    BinaryObject binaryValueObject = binaryBuilder.fromMapRecord((MapRecord) record, cacheValueType);
                    cacheItems.add(new AbstractMap.SimpleEntry<>(binaryKeyObject, binaryValueObject));
                }
            } catch (SchemaNotFoundException | MalformedRecordException e) {
                throw new ProcessException("Could not parse incoming data", e);
            }
        });

        int retries = 0;

        while (retries < maxRetries && !cacheItems.isEmpty()) {
            try {
                IgniteFuture<?> futures = igniteDataStreamer.addData(cacheItems);
                Object result = futures.get();
                getLogger().trace("Result {} of addData", new Object[]{result});
                cacheItems.clear();
                session.getProvenanceReporter().send(flowFile, "ignite://cache/" + getIgniteCache().getName() + "/");
            } catch (CacheException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IgniteClientDisconnectedException) {
                    getLogger().error("Ignite client disconnected, recreating DataStreamer", e);
                    igniteDataStreamer = null;
                    initializeIgniteDataStreamer(context);
                    retries++;
                }
            } catch (IgniteInterruptedException | IllegalStateException | IgniteDataStreamerTimeoutException e) {
                getLogger().error("Ignite cache write failure", e);
                session.transfer(flowFile, REL_FAILURE);
                context.yield();
                return;
            }
        }
        session.transfer(flowFile, REL_SUCCESS);
    }
}
