package com.yammer.collections.guava.azure;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;
import com.microsoft.windowsazure.services.core.storage.StorageErrorCode;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.TableOperation;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.yammer.collections.guava.azure.StringEntityUtil.EXTRACT_VALUE;
import static com.yammer.collections.guava.azure.StringEntityUtil.decode;
import static com.yammer.collections.guava.azure.StringEntityUtil.encode;


public class StringAzureTable implements Table<String, String, String> {
    private static final Timer GET_TIMER = createTimerFor("get");
    private static final Timer PUT_TIMER = createTimerFor("put");
    private static final Timer REMOVE_TIMER = createTimerFor("remove");
    private static final Function<StringEntity, String> COLUMN_KEY_EXTRACTOR = new Function<StringEntity, String>() {
        @Override
        public String apply(StringEntity input) {
            return decode(input.getRowKey());
        }
    };
    private final String tableName;
    private final StringTableCloudClient stringCloudTableClient;
    private final StringTableRequestFactory stringTableRequestFactory;

    /* package */ StringAzureTable(String tableName, StringTableCloudClient stringCloudTableClient, StringTableRequestFactory stringTableRequestFactory) {
        this.tableName = tableName;
        this.stringCloudTableClient = stringCloudTableClient;
        this.stringTableRequestFactory = stringTableRequestFactory;
    }

    public StringAzureTable(String secretieTableName, CloudTableClient tableClient) {
        this(secretieTableName, new StringTableCloudClient(tableClient), new StringTableRequestFactory());
    }

    private static Timer createTimerFor(String name) {
        return Metrics.newTimer(StringAzureTable.class, name);
    }

    @Override
    public boolean contains(Object rowString, Object columnString) {
        return get(rowString, columnString) != null;
    }

    @Override
    public boolean containsRow(Object rowString) {
        return (rowString instanceof String) && !row((String) rowString).isEmpty();
    }

    @Override
    public boolean containsColumn(Object columnString) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(Object value) {
        return values().contains(value); // TODO this can be optimized through a direct query
    }

    @Override
    public String get(Object rowString, Object columnString) {
        return entityToValue(rawGet(rowString, columnString));
    }

    private String entityToValue(StringEntity stringEntity) {
        return stringEntity == null ? null : decode(stringEntity.getValue());
    }

    private StringEntity rawGet(Object rowString, Object columnString) {
        if (!(rowString instanceof String && columnString instanceof String)) {
            return null;
        }

        String row = encode((String) rowString);
        String column = encode((String) columnString);

        TableOperation retrieveEntityOperation = stringTableRequestFactory.retrieve(row, column);

        try {
            return timedTableOperation(GET_TIMER, retrieveEntityOperation);
        } catch (StorageException e) {
            throw Throwables.propagate(e);
        }
    }

    private StringEntity timedTableOperation(Timer contextSpecificTimer, TableOperation tableOperation) throws StorageException {
        TimerContext context = contextSpecificTimer.time();
        try {
            return stringCloudTableClient.execute(tableName, tableOperation);
        } finally {
            context.stop();
        }
    }

    @Override
    public boolean isEmpty() {
        return cellSet().isEmpty();
    }

    @Override
    public int size() {
        return cellSet().size();
    }

    @Override
    public void clear() { // TODO do a javadoc
        for (Cell<String, String, String> cell : cellSet()) {
            remove(cell.getRowKey(), cell.getColumnKey());
        }
    }

    @Override
    public String put(String rowString, String columnString, String value) {
        TableOperation putStringieOperation = stringTableRequestFactory.put(encode(rowString), encode(columnString), encode(value));

        try {
            return entityToValue(timedTableOperation(PUT_TIMER, putStringieOperation));
        } catch (StorageException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void putAll(Table<? extends String, ? extends String, ? extends String> table) {
        for (Cell<? extends String, ? extends String, ? extends String> cell : table.cellSet()) {
            put(cell.getRowKey(), cell.getColumnKey(), cell.getValue());
        }
    }

    @Override
    public String remove(Object rowString, Object columnString) {
        StringEntity entityToBeDeleted = rawGet(rowString, columnString);

        if (entityToBeDeleted == null) {
            return null;
        }

        TableOperation deleteStringieOperation = stringTableRequestFactory.delete(entityToBeDeleted);

        try {
            return entityToValue(timedTableOperation(REMOVE_TIMER, deleteStringieOperation));
        } catch (StorageException e) {
            if (notFound(e)) {
                return null;
            }
            throw Throwables.propagate(e);
        }
    }

    private boolean notFound(StorageException e) {
        return StorageErrorCode.RESOURCE_NOT_FOUND.toString().equals(e.getErrorCode())
                || "ResourceNotFound".equals(e.getErrorCode());
    }

    @Override
    public Map<String, String> row(String rowString) {
        return new ColumnMap(this, rowString, stringCloudTableClient, stringTableRequestFactory);
    }

    @Override
    public Map<String, String> column(String columnString) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Cell<String, String, String>> cellSet() {
        return new CellSetMutableView(this, stringCloudTableClient, stringTableRequestFactory);
    }

    @Override
    public Set<String> rowKeySet() {
        throw new UnsupportedOperationException();
    }

    // TODO java doc: this is a very expensive operation, materializes all the columns in memmory: there are no agregate functions on azure
    @Override
    public Set<String> columnKeySet() {
        return ImmutableSet.copyOf(new CollectionView<>(this, COLUMN_KEY_EXTRACTOR, stringCloudTableClient, stringTableRequestFactory));
    }

    @Override
    public Collection<String> values() {
        return new CollectionView<>(this, EXTRACT_VALUE, stringCloudTableClient, stringTableRequestFactory);
    }

    @Override
    public Map<String, Map<String, String>> rowMap() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Map<String, String>> columnMap() {
        throw new UnsupportedOperationException();
    }

    public String getTableName() {
        return tableName;
    }
}
