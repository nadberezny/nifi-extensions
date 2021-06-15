package pl.touk.nifi.processors;

import org.apache.ignite.IgniteCache;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.ignite.AbstractIgniteProcessor;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractGenericIgniteCacheProcessor<K, V> extends AbstractIgniteProcessor {

    protected static final PropertyDescriptor CACHE_NAME = new PropertyDescriptor.Builder()
            .displayName("Ignite Cache Name")
            .name("ignite-cache-name")
            .description("The name of the ignite cache")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CACHE_KEY_TYPE = new PropertyDescriptor.Builder()
            .displayName("Ignite Cache Key Type")
            .name("ignite-cache-key-type")
            .description("The name of the ignite cache key type")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CACHE_VALUE_TYPE = new PropertyDescriptor.Builder()
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

    protected static Set<Relationship> relationships;

    private String cacheName;

    protected IgniteCache<K, V> getIgniteCache() {
        if ( getIgnite() == null )
            return null;
        else
            return getIgnite().getOrCreateCache(cacheName);
    }

    static {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    public void initializeIgniteCache(ProcessContext context) throws ProcessException {

        getLogger().info("Initializing Ignite cache");

        try {
            if ( getIgnite() == null ) {
                getLogger().info("Initializing ignite as client");
                super.initializeIgnite(context);
            }

            cacheName = context.getProperty(CACHE_NAME).getValue();

        } catch (Exception e) {
            getLogger().error("Failed to initialize ignite cache due to {}", new Object[] { e }, e);
            throw new ProcessException(e);
        }
    }

    @OnShutdown
    public void closeIgniteCache() {
        if (getIgniteCache() != null) {
            getLogger().info("Closing ignite cache");
            getIgniteCache().close();
        }
        super.closeIgnite();
    }
}
