package pl.touk.nifi.services;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.RecordLookupService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.ResultSetRecordSet;
import pl.touk.nifi.services.lookup.sql.LookupSqlArgument;
import pl.touk.nifi.services.lookup.sql.LookupSqlQuery;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Tags({"lookup", "cache", "enrich", "join", "rdbms", "database", "query", "sql", "reloadable", "record"})
@CapabilityDescription("A relational-database-based lookup service. When the lookup record is found in the database, "
        + "the specified columns (or all if Lookup Value Columns are not specified) are returned as a Record. Only one row "
        + "will be returned for each lookup, duplicate database entries are ignored.")
public class DatabaseRecordLookupService extends AbstractDatabaseLookupService implements RecordLookupService {

    private volatile Cache<LookupSqlQuery, Record> cache;

    protected static final String ARG_PREFIX = "arg";

    protected static final String ARGS_PATTERN = "^arg[0-9]+$";

    static final Set<String> REQUIRED_KEYS = Collections.emptySet();

    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-dbcp-service")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-table-name")
            .displayName("Table Name")
            .description("The name of the database table to be queried. Note that this may be case-sensitive depending on the database.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor LOOKUP_VALUE_COLUMNS = new PropertyDescriptor.Builder()
            .name("dbrecord-lookup-value-columns")
            .displayName("Lookup Value Columns")
            .description("A comma-delimited list of columns in the table that will be returned when the lookup key matches. Note that this may be case-sensitive depending on the database.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    protected static final PropertyDescriptor WHERE_CLAUSE = new PropertyDescriptor.Builder()
            .name("where-clause")
            .displayName("Where")
            .description("Where clause, eg. age > ?")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    protected List<PropertyDescriptor> properties;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DBCP_SERVICE);
        properties.add(TABLE_NAME);
        properties.add(LOOKUP_VALUE_COLUMNS);
        properties.add(WHERE_CLAUSE);
        properties.add(CACHE_SIZE);
        properties.add(CLEAR_CACHE_ON_ENABLED);
        properties.add(CACHE_EXPIRATION);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        int cacheSize = context.getProperty(CACHE_SIZE).evaluateAttributeExpressions().asInteger();
        boolean clearCache = context.getProperty(CLEAR_CACHE_ON_ENABLED).asBoolean();
        long durationNanos = context.getProperty(CACHE_EXPIRATION).isSet() ? context.getProperty(CACHE_EXPIRATION).evaluateAttributeExpressions().asTimePeriod(TimeUnit.NANOSECONDS) : 0L;
        if (this.cache == null || (cacheSize > 0 && clearCache)) {
            if (durationNanos > 0) {
                this.cache = Caffeine.newBuilder()
                        .maximumSize(cacheSize)
                        .expireAfter(new Expiry<LookupSqlQuery, Record>() {
                            @Override
                            public long expireAfterCreate(LookupSqlQuery query, Record record, long currentTime) {
                                return durationNanos;
                            }

                            @Override
                            public long expireAfterUpdate(LookupSqlQuery query, Record record, long currentTime, long currentDuration) {
                                return currentDuration;
                            }

                            @Override
                            public long expireAfterRead(LookupSqlQuery query, Record record, long currentTime, long currentDuration) {
                                return currentDuration;
                            }
                        })
                        .build();
            } else {
                this.cache = Caffeine.newBuilder()
                        .maximumSize(cacheSize)
                        .build();
            }
        }

    }

    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        return lookup(coordinates, null);
    }

    @Override
    public Optional<Record> lookup(final Map<String, Object> coordinates, Map<String, String> context) throws LookupFailureException {
        if (coordinates == null) {
            return Optional.empty();
        }

        String tableName = getProperty(TABLE_NAME).evaluateAttributeExpressions(context).getValue();
        String lookupValueColumnsList = getProperty(LOOKUP_VALUE_COLUMNS).evaluateAttributeExpressions(context).getValue();
        String whereClause = getProperty(WHERE_CLAUSE).evaluateAttributeExpressions(context).getValue();
        Set<String> lookupValueColumnsSet = new LinkedHashSet<>();
        if (lookupValueColumnsList != null) {
            Stream.of(lookupValueColumnsList)
                    .flatMap(path -> Arrays.stream(path.split(",")))
                    .filter(DatabaseRecordLookupService::isNotBlank)
                    .map(String::trim)
                    .forEach(lookupValueColumnsSet::add);
        }
        LookupSqlQuery query = new LookupSqlQuery(lookupValueColumnsSet, tableName, whereClause, getQueryArgs(coordinates));

        Record recordFromCache = cache.get(query, k -> null);
        if (recordFromCache == null) {
            Connection conn = dbcpService.getConnection(context);
            try {
                PreparedStatement statement = query.toStatement(conn);
                ResultSet resultSet = statement.executeQuery();
                ResultSetRecordSet resultSetRecordSet = new ResultSetRecordSet(resultSet, null);
                Record record = resultSetRecordSet.next();
                if (record != null) {
                    cache.put(query, record);
                }
                return Optional.ofNullable(record);
            } catch (SQLException se) {
                throw new LookupFailureException("Error executing SQL statement. " + (se.getCause() == null ? se.getMessage() : se.getCause().getMessage()), se);
            } catch (IOException ioe) {
                throw new LookupFailureException("Error retrieving result set for SQL statement. " + (ioe.getCause() == null ? ioe.getMessage() : ioe.getCause().getMessage()), ioe);
            }
        } else {
            return Optional.of(recordFromCache);
        }
    }

    private static boolean isNotBlank(final String value) {
        return value != null && !value.trim().isEmpty();
    }

    private Set<LookupSqlArgument> getQueryArgs(Map<String, Object> coordinates) {
        return coordinates.entrySet().stream()
                .filter(entry -> entry.getKey().matches(ARGS_PATTERN))
                .map(entry -> {
                    int argIx = Integer.parseInt((entry.getKey()).replaceFirst(ARG_PREFIX, ""));
                    return new LookupSqlArgument(argIx, entry.getValue());
                })
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }
}
