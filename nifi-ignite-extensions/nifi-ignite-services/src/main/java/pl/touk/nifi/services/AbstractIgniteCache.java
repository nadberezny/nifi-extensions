package pl.touk.nifi.services;

import org.apache.ignite.client.ClientCache;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public abstract class AbstractIgniteCache<K, V> extends AbstractIgniteClient {

    protected static final PropertyDescriptor CACHE_NAME = new PropertyDescriptor.Builder()
            .displayName("Ignite Cache Name")
            .name("ignite-cache-name")
            .description("The name of the ignite cache")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected volatile String cacheName;

    private volatile ClientCache<K, V> cache;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Stream.concat(
                super.getSupportedPropertyDescriptors().stream(),
                Stream.of(CACHE_NAME).collect(Collectors.toList()).stream()
        ).collect(Collectors.toList());
    }

    @Override @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        super.onEnabled(context);
        initializeIgniteCache(context);
    }

    protected ClientCache<K, V> getCache() {
        return cache;
    }

    public void initializeIgniteCache(ConfigurationContext context) {
        cacheName = context.getProperty(CACHE_NAME).getValue();
        getLogger().info("Initializing Ignite cache: " +  cacheName + " for " + context.getName());
        cache = getIgniteClient().getOrCreateCache(cacheName).withKeepBinary();
    }
}
