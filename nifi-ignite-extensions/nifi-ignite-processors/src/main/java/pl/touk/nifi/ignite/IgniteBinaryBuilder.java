package pl.touk.nifi.ignite;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.nifi.serialization.record.MapRecord;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class IgniteBinaryBuilder {

    private IgniteBinary igniteBinary;

    public IgniteBinaryBuilder(IgniteBinary igniteBinary) {
        this.igniteBinary = igniteBinary;
    }

    public BinaryObject fromMapRecord(MapRecord record, String igniteTypeName) {
        return fromMapRecord(record, igniteTypeName, null);
    }

    public BinaryObject fromMapRecord(MapRecord record, String igniteTypeName, String[] fieldNamesIncluded) {
        BinaryObjectBuilder builder = igniteBinary.builder(igniteTypeName);
        getRecordFields(record, fieldNamesIncluded).forEach(builder::setField);
        return builder.build();
    }

    private Map<String, Object> getRecordFields(MapRecord record, String[] fieldNamesIncluded) {
        if (fieldNamesIncluded == null)
            return record.toMap();
        else {
            return record.toMap().entrySet().stream().filter(recordEntry ->
                    Arrays.stream(fieldNamesIncluded).anyMatch(fieldName ->
                            recordEntry.getKey().equals(fieldName))
            ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }
}
