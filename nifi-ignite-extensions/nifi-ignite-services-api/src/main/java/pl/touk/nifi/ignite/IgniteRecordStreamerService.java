package pl.touk.nifi.ignite;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.List;


public interface IgniteRecordStreamerService extends ControllerService {

    DataStreamerResult addDataSync(RecordSchema schema, List<MapRecord> records);
}
