package pl.touk.nifi.services;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryObject;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordSchema;
import pl.touk.nifi.ignite.DataStreamerResult;
import pl.touk.nifi.ignite.IgniteBinaryBuilder;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IgniteBinaryKeyBinaryObjectStreamer extends AbstractIgniteRecordStreamerService<BinaryObject, BinaryObject> {

    public static final PropertyDescriptor CACHE_KEY_TYPE = new PropertyDescriptor.Builder()
            .displayName("Ignite Cache Key Type")
            .name("ignite-cache-key-type")
            .description("The name of the ignite cache key type")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CACHE_VALUE_TYPE = new PropertyDescriptor.Builder()
            .displayName("Ignite Cache Value Type")
            .name("ignite-cache-value-type")
            .description("The name of the ignite cache value type")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEY_FIELD_NAMES = new PropertyDescriptor.Builder()
            .displayName("Key field names")
            .name("key-field-names")
            .description("Comma-separated list of record fields that compose Ignite key")
            .required(true)
            .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.NON_EMPTY_VALIDATOR))
            .build();

    protected static final List<PropertyDescriptor> descriptors =
            Arrays.asList(
                    IGNITE_THICK_CLIENT_SERVICE, CACHE_NAME, CACHE_KEY_TYPE, CACHE_VALUE_TYPE, KEY_FIELD_NAMES,
                    DATA_STREAMER_PER_NODE_PARALLEL_OPERATIONS, DATA_STREAMER_PER_NODE_BUFFER_SIZE, DATA_STREAMER_AUTO_FLUSH_FREQUENCY,
                    DATA_STREAMER_ALLOW_OVERRIDE
            );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public DataStreamerResult addDataSync(RecordSchema schema, List<MapRecord> records) {
        final List<String> keyFieldNames = Arrays.asList(configContext.getProperty(KEY_FIELD_NAMES).getValue().split(","));
        final String cacheKeyType = configContext.getProperty(CACHE_KEY_TYPE).getValue();
        final String cacheValueType = configContext.getProperty(CACHE_VALUE_TYPE).getValue();
        final IgniteBinary igniteBinary = thickClient.getIgnite().binary();
        final IgniteBinaryBuilder binaryKeyBuilder = new IgniteBinaryBuilder(igniteBinary, cacheKeyType, keyFieldNames);
        final IgniteBinaryBuilder binaryValueBuilder = new IgniteBinaryBuilder(igniteBinary, cacheValueType, schema.getFieldNames());

        List<Map.Entry<BinaryObject, BinaryObject>> entries = records.stream().map(record -> {
            BinaryObject binaryKeyObject = binaryKeyBuilder.build(record);
            BinaryObject binaryValueObject = binaryValueBuilder.build(record);
            return new AbstractMap.SimpleEntry<>(binaryKeyObject, binaryValueObject);
        }).collect(Collectors.toList());

        return addEntries(entries);
    }
}
