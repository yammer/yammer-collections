package com.yammer.collections.guava.azure;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Table;
import com.microsoft.windowsazure.services.core.storage.StorageErrorCode;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.TableOperation;
import com.microsoft.windowsazure.services.table.client.TableQuery;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.yammer.collections.guava.azure.AzureEntityUtil.EXTRACT_VALUE;
import static com.yammer.collections.guava.azure.AzureEntityUtil.decode;
import static com.yammer.collections.guava.azure.AzureEntityUtil.encode;

public class BaseAzureTable implements Table<String, String, String> {
    private static final Function<AzureEntity, String> COLUMN_KEY_EXTRACTOR = new Function<AzureEntity, String>() {
        @Override
        public String apply(AzureEntity input) {
            return decode(input.getRowKey());
        }
    };
    private static final Function<AzureEntity, String> ROW_KEY_EXTRACTOR = new Function<AzureEntity, String>() {
        @Override
        public String apply(AzureEntity input) {
            return decode(input.getPartitionKey());
        }
    };
    private final String tableName;
    private final AzureTableCloudClient stringCloudTableClient;
    private final AzureTableRequestFactory azureTableRequestFactory;

    /* package */ BaseAzureTable(String tableName, AzureTableCloudClient stringCloudTableClient, AzureTableRequestFactory azureTableRequestFactory) {
        this.tableName = tableName;
        this.stringCloudTableClient = stringCloudTableClient;
        this.azureTableRequestFactory = azureTableRequestFactory;
    }

    public BaseAzureTable(String secretieTableName, CloudTableClient tableClient) {
        this(secretieTableName, new AzureTableCloudClient(tableClient), new AzureTableRequestFactory());
    }

    private static String entityToValue(AzureEntity azureEntity) {
        return azureEntity == null ? null : decode(azureEntity.getValue());
    }

    private static boolean notFound(StorageException e) {
        return StorageErrorCode.RESOURCE_NOT_FOUND.toString().equals(e.getErrorCode())
                || "ResourceNotFound".equals(e.getErrorCode());
    }

    @Override
    public boolean contains(Object rowString, Object columnString) {
        return get(rowString, columnString) != null;
    }

    @Override
    public boolean containsRow(Object rowString) {
        return rowString instanceof String && !row((String) rowString).isEmpty();
    }

    @Override
    public boolean containsColumn(Object columnString) {
        return columnString instanceof String && !column((String) columnString).isEmpty();
    }

    @Override
    public boolean containsValue(Object value) {
        if (!(value instanceof String)) {
            return false;
        }

        TableQuery<AzureEntity> valueQuery = azureTableRequestFactory.containsValueQuery(tableName, encode((String) value));
        return stringCloudTableClient.execute(valueQuery).iterator().hasNext();
    }

    @Override
    public String get(Object rowString, Object columnString) {
        return entityToValue(rawGet(rowString, columnString));
    }

    private AzureEntity rawGet(Object rowString, Object columnString) {
        if (!(rowString instanceof String && columnString instanceof String)) {
            return null;
        }

        String row = encode((String) rowString);
        String column = encode((String) columnString);

        TableOperation retrieveEntityOperation = azureTableRequestFactory.retrieve(row, column);

        try {
            return stringCloudTableClient.execute(tableName, retrieveEntityOperation);
        } catch (StorageException e) {
            throw Throwables.propagate(e);
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
    public void clear() {
        for (Cell<String, String, String> cell : cellSet()) {
            remove(cell.getRowKey(), cell.getColumnKey());
        }
    }

    @Override
    public String put(String rowString, String columnString, String value) {
        TableOperation putStringieOperation = azureTableRequestFactory.put(encode(rowString), encode(columnString), encode(value));

        try {
            return entityToValue(stringCloudTableClient.execute(tableName, putStringieOperation));
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
        AzureEntity entityToBeDeleted = rawGet(rowString, columnString);

        if (entityToBeDeleted == null) {
            return null;
        }

        TableOperation deleteStringieOperation = azureTableRequestFactory.delete(entityToBeDeleted);

        try {
            return entityToValue(stringCloudTableClient.execute(tableName, deleteStringieOperation));
        } catch (StorageException e) {
            if (notFound(e)) {
                return null;
            }
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Map<String, String> row(String rowString) {
        return new ColumnView(this, rowString, stringCloudTableClient, azureTableRequestFactory);
    }

    @Override
    public Map<String, String> column(String columnString) {
        return new RowView(this, columnString, stringCloudTableClient, azureTableRequestFactory);
    }

    @Override
    public Set<Cell<String, String, String>> cellSet() {
        return new CellSetMutableView(this, stringCloudTableClient, azureTableRequestFactory);
    }

    @Override
    public Set<String> rowKeySet() {
        return SetView.fromCollectionView(
                new TableCollectionView<>(this, ROW_KEY_EXTRACTOR, stringCloudTableClient, azureTableRequestFactory)
        );
    }

    @Override
    public Set<String> columnKeySet() {
        return SetView.fromCollectionView(
                new TableCollectionView<>(this, COLUMN_KEY_EXTRACTOR, stringCloudTableClient, azureTableRequestFactory)
        );
    }

    @Override
    public Collection<String> values() {
        return new TableCollectionView<>(this, EXTRACT_VALUE, stringCloudTableClient, azureTableRequestFactory);
    }

    @Override
    public Map<String, Map<String, String>> rowMap() {
        return new RowMapView(this);
    }

    @Override
    public Map<String, Map<String, String>> columnMap() {
        return new ColumnMapView<>(this);
    }

    public String getTableName() {
        return tableName;
    }

    private static final class TableCollectionView<E> extends AbstractCollectionView<E> {
        private final BaseAzureTable baseAzureTable;
        private final AzureTableCloudClient azureTableCloudClient;
        private final AzureTableRequestFactory azureTableRequestFactory;

        public TableCollectionView(BaseAzureTable baseAzureTable, Function<AzureEntity, E> typeExtractor, AzureTableCloudClient azureTableCloudClient, AzureTableRequestFactory azureTableRequestFactory) {
            super(typeExtractor);
            this.baseAzureTable = baseAzureTable;
            this.azureTableCloudClient = azureTableCloudClient;
            this.azureTableRequestFactory = azureTableRequestFactory;
        }

        @Override
        protected Iterable<AzureEntity> getBackingIterable() {
            TableQuery<AzureEntity> query = azureTableRequestFactory.selectAll(baseAzureTable.getTableName());
            return azureTableCloudClient.execute(query);
        }
    }
}
