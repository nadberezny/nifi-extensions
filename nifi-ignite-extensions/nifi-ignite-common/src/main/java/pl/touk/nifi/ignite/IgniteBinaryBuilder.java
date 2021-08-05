package pl.touk.nifi.ignite;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.nifi.serialization.record.MapRecord;

import java.util.List;

public class IgniteBinaryBuilder {

    private IgniteBinary igniteBinary;

    private String igniteTypeName;

    private List<String> fieldNames;

    public IgniteBinaryBuilder(IgniteBinary igniteBinary, String igniteTypeName, List<String> fieldNames) {
        this.igniteBinary = igniteBinary;
        this.igniteTypeName = igniteTypeName;
        this.fieldNames = fieldNames;
    }

    public BinaryObject build(MapRecord record) {
        BinaryObjectBuilder builder = igniteBinary.builder(igniteTypeName);
        fieldNames.forEach(fieldName -> builder.setField(fieldName, record.getValue(fieldName)));
        return builder.build();
    }
}
