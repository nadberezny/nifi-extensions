package pl.touk.nifi.services;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractIgniteClient extends AbstractControllerService {

    public static final PropertyDescriptor SERVER_ADDRESSES = new PropertyDescriptor.Builder()
            .name("ignite-addresses")
            .displayName("Ignite addresses")
            .description("Comma-separated list of Ignite host addresses for thin client, eg. host1:10800,host2:10800")
            .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.NON_EMPTY_VALIDATOR))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("ignite-username")
            .displayName("Ignite user")
            .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.NON_EMPTY_VALIDATOR))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("ignite-password")
            .displayName("Ignite password")
            .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.NON_EMPTY_VALIDATOR))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .sensitive(true)
            .build();

    private transient IgniteClient igniteClient;
    
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Stream.of(SERVER_ADDRESSES, USERNAME, PASSWORD).collect(Collectors.toList());
    }

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
        
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        initAndStartClient(context);
    }

    @OnDisabled
    public void onDisabled() throws Exception {
       closeIgniteClient();
    }

    protected IgniteClient getIgniteClient() {
        return igniteClient;
    }

    protected void initAndStartClient(ConfigurationContext context) throws ClientException {
        if (igniteClient != null) {
            getLogger().info("Ignite already initialized");
            return;
        }
        String[] hostAddresses = context.getProperty(SERVER_ADDRESSES).getValue().split(",");
        String username = context.getProperty(USERNAME).getValue();
        String password = context.getProperty(USERNAME).getValue();
        synchronized (Ignition.class) {
            ClientConfiguration cfg = new ClientConfiguration().setAddresses(hostAddresses);
            if (username != null) {
                cfg.setUserName(username);
            }
            if (password != null) {
                cfg.setUserPassword(password);
            }

            getLogger().info("Initializing Ignite thin client for " + context.getName());
            igniteClient = Ignition.startClient(cfg);
        }
    }

    protected void closeIgniteClient() throws Exception {
        if (igniteClient != null) {
            getLogger().info("Closing ignite client");
            igniteClient.close();
            igniteClient = null;
        }
    }
}