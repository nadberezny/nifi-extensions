package pl.touk.nifi.services.typing;

import org.apache.ignite.binary.BinaryType;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class IgniteTyping {

    private static final Map<String , DataType> IGNITE_TO_NIFI = new HashMap<String , DataType>() {{
        put("byte", RecordFieldType.BYTE.getDataType());
        put("short", RecordFieldType.SHORT.getDataType());
        put("int", RecordFieldType.INT.getDataType());
        put("long", RecordFieldType.LONG.getDataType());
        put("boolean", RecordFieldType.BOOLEAN.getDataType());
        put("float", RecordFieldType.FLOAT.getDataType());
        put("double", RecordFieldType.DOUBLE.getDataType());
        put("decimal", RecordFieldType.DECIMAL.getDataType());
        put("String", RecordFieldType.STRING.getDataType());
        put("Date", RecordFieldType.DATE.getDataType());
        put("Timestamp", RecordFieldType.TIMESTAMP.getDataType());
        put("Time", RecordFieldType.TIME.getDataType());
    }};

    public static final String SUPPORTED_IGNITE_TYPE_NAMES = String.join(",", IGNITE_TO_NIFI.keySet());

    public static RecordField get(String binaryFieldTypeName) throws IllegalArgumentException {
        DataType nifiFieldType = IGNITE_TO_NIFI.get(binaryFieldTypeName);
        if (nifiFieldType == null) {
            throw new IllegalArgumentException("Ignite type: " + binaryFieldTypeName + " is not supported. Supported types are: " + SUPPORTED_IGNITE_TYPE_NAMES);
        }
        return new RecordField(binaryFieldTypeName, nifiFieldType);
    }
}
