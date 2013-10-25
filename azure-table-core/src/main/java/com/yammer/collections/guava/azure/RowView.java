package com.yammer.collections.guava.azure;


import com.google.common.base.Function;
import com.microsoft.windowsazure.services.table.client.TableQuery;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.yammer.collections.guava.azure.AzureEntityUtil.EXTRACT_VALUE;
import static com.yammer.collections.guava.azure.AzureEntityUtil.decode;
import static com.yammer.collections.guava.azure.AzureEntityUtil.encode;

public class RowView implements Map<String, String> {
    private static final Function<AzureEntity, String> EXTRACT_ROW_KEY = new Function<AzureEntity, String>() {
        @Override
        public String apply(AzureEntity input) {
            return decode(input.getPartitionKey());
        }
    };
    private final BaseAzureTable baseAzureTable;
    private final String columnKey;
    private final AzureTableCloudClient azureTableCloudClient;
    private final AzureTableRequestFactory azureTableRequestFactory;
    private final Function<AzureEntity, Entry<String, String>> extractEntry;

    public RowView(
            final BaseAzureTable baseAzureTable,
            final String columnKey,
            AzureTableCloudClient azureTableCloudClient,
            AzureTableRequestFactory azureTableRequestFactory) {
        this.baseAzureTable = baseAzureTable;
        this.columnKey = columnKey;
        this.azureTableCloudClient = azureTableCloudClient;
        this.azureTableRequestFactory = azureTableRequestFactory;
        extractEntry = new Function<AzureEntity, Entry<String, String>>() {
            @Override
            public Entry<String, String> apply(AzureEntity input) {
                return new RowMapEntry(decode(input.getPartitionKey()), columnKey, baseAzureTable);
            }
        };
    }

    @Override // TODO this requires a javadoc to explain that this is a very expensive operation
    public int size() {
        return entrySet().size();
    }

    @Override
    public boolean isEmpty() {
        return entrySet().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return baseAzureTable.contains(key, columnKey);
    }

    @Override
    public boolean containsValue(Object value) {
        if (!(value instanceof String)) {
            return false;
        }

        TableQuery<AzureEntity> valueQuery = azureTableRequestFactory.containsValueForColumnQuery(baseAzureTable.getTableName(), encode(columnKey),
                encode((String) value));
        return azureTableCloudClient.execute(valueQuery).iterator().hasNext();
    }

    @Override
    public String get(Object key) {
        return baseAzureTable.get(key, columnKey);
    }

    @Override
    public String put(String key, String value) {
        return baseAzureTable.put(key, columnKey, value);
    }

    @Override
    public String remove(Object key) {
        return baseAzureTable.remove(key, columnKey);
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        for (Entry<? extends String, ? extends String> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {// TODO this requires a javadoc to explain that this is a very expensive operation
        for (String rowKey : keySet()) {
            remove(rowKey);
        }
    }

    @Override
    public Set<String> keySet() {
        // TODO this should have a view that is mutable (makes sense)
        return
                SetView.fromSetCollectionView(
                        new RowMapSetView<>(baseAzureTable, columnKey, EXTRACT_ROW_KEY, azureTableCloudClient, azureTableRequestFactory)
                );
    }

    @Override
    public Collection<String> values() {
        return new RowMapSetView<>(baseAzureTable, columnKey, EXTRACT_VALUE, azureTableCloudClient, azureTableRequestFactory);
    }

    @Override
    public Set<Entry<String, String>> entrySet() { // TODO this should have a view that is mutable (makes sense)
        return SetView.fromSetCollectionView(
                new RowMapSetView<>(baseAzureTable, columnKey, extractEntry, azureTableCloudClient, azureTableRequestFactory)
        );
    }

    private static class RowMapEntry implements Entry<String, String> {
        private final String columnKey;
        private final String rowKey;
        private final BaseAzureTable azureTable;

        private RowMapEntry(String rowKey, String columnKey, BaseAzureTable azureTable) {
            this.rowKey = rowKey;
            this.columnKey = columnKey;
            this.azureTable = azureTable;
        }

        @Override
        public String getKey() {
            return rowKey;
        }

        @Override
        public String getValue() {
            return azureTable.get(rowKey, columnKey);
        }

        @Override
        public String setValue(String value) {
            return azureTable.put(rowKey, columnKey, value);
        }
    }

    private static class RowMapSetView<E> extends CollectionView<E> {
        private final BaseAzureTable baseAzureTable;
        private final String columnKey;
        private final AzureTableCloudClient azureTableCloudClient;
        private final AzureTableRequestFactory azureTableRequestFactory;

        public RowMapSetView(
                BaseAzureTable baseAzureTable,
                String columnKey,
                Function<AzureEntity, E> typeExtractor,
                AzureTableCloudClient azureTableCloudClient,
                AzureTableRequestFactory azureTableRequestFactory) {
            super(typeExtractor);
            this.baseAzureTable = baseAzureTable;
            this.columnKey = columnKey;
            this.azureTableCloudClient = azureTableCloudClient;
            this.azureTableRequestFactory = azureTableRequestFactory;
        }

        @Override
        protected Iterable<AzureEntity> getBackingIterable() {
            TableQuery<AzureEntity> selectAllForRowQuery = azureTableRequestFactory.selectAllForColumn(baseAzureTable.getTableName(), encode(columnKey));
            return azureTableCloudClient.execute(selectAllForRowQuery);
        }
    }

}
