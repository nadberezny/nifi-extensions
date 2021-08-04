package pl.touk.nifi.services;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public abstract class AbstractIgniteClientCache<K, V> extends AbstractIgniteThinClient {

    protected static final PropertyDescriptor CACHE_NAME = new PropertyDescriptor.Builder()
            .displayName("Ignite Cache Name")
            .name("ignite-cache-name")
            .description("The name of the ignite cache")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor BACKUPS = new PropertyDescriptor.Builder()
            .displayName("Backups")
            .name("backups-no")
            .description("The number of partition backups")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    protected static final PropertyDescriptor CACHE_MODE = new PropertyDescriptor.Builder()
            .displayName("Cache mode")
            .name("cache-mode")
            .required(true)
            .allowableValues(CacheMode.PARTITIONED.name(), CacheMode.REPLICATED.name())
            .defaultValue(CacheMode.PARTITIONED.name())
            .build();

    protected static final PropertyDescriptor DATA_REGION = new PropertyDescriptor.Builder()
            .displayName("Data region")
            .name("data-region")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    protected volatile String cacheName;

    private volatile ClientCache<K, V> cache;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Stream.concat(
                super.getSupportedPropertyDescriptors().stream(),
                Stream.of(CACHE_NAME, BACKUPS, CACHE_MODE, DATA_REGION).collect(Collectors.toList()).stream()
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
        Integer backupsNo = context.getProperty(BACKUPS).asInteger();
        String cacheMode = context.getProperty(CACHE_MODE).getValue();
        String dataRegion = context.getProperty(DATA_REGION).getValue();

        ClientCacheConfiguration cacheConfiguration = new ClientCacheConfiguration()
                .setName(cacheName)
                .setBackups(backupsNo)
                .setCacheMode(CacheMode.valueOf(cacheMode));
        if (dataRegion != null) {
            cacheConfiguration.setDataRegionName(dataRegion);
        }

        getLogger().info("Initializing Ignite cache: " +  cacheName + " for " + context.getName());
        cache = getIgniteClient().getOrCreateCache(cacheConfiguration).withKeepBinary();
    }
}
