package pl.touk.nifi.processors;

import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import pl.touk.nifi.ignite.DataStreamerResult;
import pl.touk.nifi.ignite.IgniteRecordStreamerService;

import java.util.*;

@EventDriven
@SupportsBatching
@Tags({ "Ignite", "insert", "update", "stream", "write", "put", "cache", "key", "record" })
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Puts records from FlowFile into Ignite Cache using DataStreamer.")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutIgniteRecord extends AbstractProcessor {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .displayName("Record Reader")
            .name("record-reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor DATA_STREAMER_SERVICE = new PropertyDescriptor.Builder()
            .name("ignite-data-streamer-service")
            .displayName("IgniteDataStreamer service")
            .required(true)
            .identifiesControllerService(IgniteRecordStreamerService.class)
            .build();

    protected static final List<PropertyDescriptor> descriptors = Arrays.asList(DATA_STREAMER_SERVICE, RECORD_READER);

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to Ignite cache are routed to this relationship").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to Ignite cache are routed to this relationship").build();

    public static final Relationship REL_CLIENT_CONNECTION_FAILURE = new Relationship.Builder().name("connection failure")
            .build();

    protected static Set<Relationship> relationships;

    static {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        rels.add(REL_CLIENT_CONNECTION_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final IgniteRecordStreamerService dataStreamerService = context.getProperty(DATA_STREAMER_SERVICE).asControllerService(IgniteRecordStreamerService.class);
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);


        DataStreamerResult dataStreamerResult = new DataStreamerResult();

        session.read(flowFile, in -> {
            try(final RecordReader reader = readerFactory.createRecordReader(flowFile.getAttributes(), in, flowFile.getSize(), getLogger())) {
                RecordSchema schema = reader.getSchema();
                List<MapRecord> cacheItems = new ArrayList<>();
                Record record;
                while ((record = reader.nextRecord()) != null) {
                    cacheItems.add((MapRecord) record);
                }
                dataStreamerResult.setError(dataStreamerService.addDataSync(schema, cacheItems).getError());

            } catch (SchemaNotFoundException | MalformedRecordException e) {
                throw new ProcessException("Could not parse incoming data", e);
            }
        });

        if (dataStreamerResult.isSuccess()) {
            session.transfer(flowFile, REL_SUCCESS);
        } else if (dataStreamerResult.getError().getCause() instanceof IgniteClientDisconnectedException) {
            session.transfer(flowFile, REL_CLIENT_CONNECTION_FAILURE);
        } else {
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
