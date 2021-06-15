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
        Map<String, Object> recordFields;
        if (fieldNamesIncluded == null)
            recordFields = record.toMap();
        else {
            recordFields = record.toMap().entrySet().stream().filter(recordEntry ->
                    Arrays.stream(fieldNamesIncluded).anyMatch(fieldName ->
                            recordEntry.getKey().equals(fieldName))
            ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        BinaryObjectBuilder builder = igniteBinary.builder(igniteTypeName);
        recordFields.forEach(builder::setField);
        return builder.build();
    }
}
