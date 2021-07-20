package pl.touk.nifi.services;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.util.Tuple;
import pl.touk.nifi.services.typing.IgniteTyping;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IgniteLookupService extends AbstractIgniteClientCache<String, BinaryObject> implements RecordLookupService {

    protected static final String KEY = "key";

    private static final Set<String> REQUIRED_KEYS = Collections.unmodifiableSet(Stream.of(KEY).collect(Collectors.toSet()));

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        if (coordinates == null) {
            return Optional.empty();
        }

        final String key = (String)coordinates.get(KEY);

        BinaryObject binaryObject = getCache().get(key);

        if (binaryObject == null) {
            return Optional.empty();
        }

        BinaryType binaryType = binaryObject.type();
        Collection<String> fieldNames = binaryType.fieldNames();

        List<RecordField> recordTyping = fieldNames.stream().map(fieldName -> {
            String igniteTypeName = binaryType.fieldTypeName(fieldName);
            return IgniteTyping.get(igniteTypeName);
        }).collect(Collectors.toList());

        SimpleRecordSchema recordSchema = new SimpleRecordSchema(recordTyping);

        MapRecord foundRecord = new MapRecord(recordSchema, getRecordFields(binaryObject, fieldNames));
        return Optional.of(foundRecord);
    }

    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }

//    private RecordSchema getRecordSchema()

    private Map<String, Object> getRecordFields(BinaryObject obj, Collection<String> fieldNames) {
        return fieldNames.stream()
                .map(fieldName -> new Tuple<String, Object>(fieldName, obj.field(fieldName)))
                .collect(Collectors.toMap(Tuple::getKey, Tuple::getValue));
    }
}
