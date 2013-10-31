package com.yammer.collections.guava.azure;


import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.core.storage.utils.Base64;
import com.microsoft.windowsazure.services.table.client.TableOperation;
import com.microsoft.windowsazure.services.table.client.TableQuery;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class AzureTestUtil {
    static final Function<Table.Cell<String, String, String>, AzureEntity> ENCODE_CELL = new Function<Table.Cell<String, String, String>, AzureEntity>() {
        @Override
        public AzureEntity apply(Table.Cell<String, String, String> input) {
            return encodedStringEntity(input);
        }
    };
    private static final String ENCODING = "UTF-8";

    private AzureTestUtil() {
    }

    @SafeVarargs
    static void setAzureTableToContain(String tableName,
                                       AzureTableRequestFactory azureTableRequestFactoryMock,
                                       AzureTableCloudClient azureTableCloudClientMock,
                                       Table.Cell<String, String, String>... cells) throws StorageException {
        // retrieve setup in general
        TableOperation blanketRetrieveOperationMock = mock(TableOperation.class);
        when(azureTableRequestFactoryMock.retrieve(any(String.class), any(String.class))).thenReturn(blanketRetrieveOperationMock);


        // per entity setup
        TableQuery<AzureEntity> emptyQuery = mock(TableQuery.class);
        when(azureTableRequestFactoryMock.containsValueQuery(anyString(), anyString())).thenReturn(emptyQuery);
        when(azureTableCloudClientMock.execute(emptyQuery)).thenReturn(Collections.<AzureEntity>emptyList());
        Collection<AzureEntity> encodedStringEntities = Lists.newArrayList();
        for (Table.Cell<String, String, String> cell : cells) {
            encodedStringEntities.add(encodedStringEntity(cell));
            setAzureTableToRetrieve(tableName, azureTableRequestFactoryMock, azureTableCloudClientMock, cell);

            TableQuery<AzureEntity> valueQuery = mock(TableQuery.class);
            when(azureTableRequestFactoryMock.containsValueQuery(tableName, encode(cell.getValue()))).thenReturn(valueQuery);
            when(azureTableCloudClientMock.execute(valueQuery)).thenReturn(Collections.singleton(ENCODE_CELL.apply(cell)));
        }

        // select query
        TableQuery<AzureEntity> tableQuery = mock(TableQuery.class);
        when(azureTableRequestFactoryMock.selectAll(tableName)).thenReturn(tableQuery);
        when(azureTableCloudClientMock.execute(tableQuery)).thenReturn(encodedStringEntities);

        setupRowQueries(tableName, azureTableRequestFactoryMock, azureTableCloudClientMock, cells);
        setupColumnQueries(tableName, azureTableRequestFactoryMock, azureTableCloudClientMock, cells);
    }

    static String encode(String stringToBeEncoded) {
        try {
            return Base64.encode(stringToBeEncoded.getBytes(ENCODING));
        } catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }
    }

    static AzureEntity encodedStringEntity(Table.Cell<String, String, String> unEncodedcell) {
        return new AzureEntity(encode(unEncodedcell.getRowKey()), encode(unEncodedcell.getColumnKey()), encode(unEncodedcell.getValue()));
    }

    private static void setAzureTableToRetrieve(
            String tableName,
            AzureTableRequestFactory azureTableRequestFactoryMock,
            AzureTableCloudClient azureTableCloudClientMock,
            Table.Cell<String, String, String> cell) throws StorageException {
        TableOperation retriveTableOperationMock = mock(TableOperation.class);
        when(azureTableRequestFactoryMock.retrieve(encode(cell.getRowKey()), encode(cell.getColumnKey()))).thenReturn(retriveTableOperationMock);
        when(azureTableCloudClientMock.execute(tableName, retriveTableOperationMock)).thenReturn(encodedStringEntity(cell));
    }

    @SafeVarargs
    private static void setupRowQueries(String tableName,
                                        AzureTableRequestFactory azureTableRequestFactoryMock,
                                        AzureTableCloudClient azureTableCloudClientMock,
                                        Table.Cell<String, String, String>... cells) {

        TableQuery<AzureEntity> emptyQueryMock = mock(TableQuery.class);
        when(azureTableRequestFactoryMock.selectAllForRow(anyString(), anyString())).thenReturn(emptyQueryMock);
        when(azureTableRequestFactoryMock.containsValueForRowQuery(anyString(), anyString(), anyString())).thenReturn(emptyQueryMock);
        when(azureTableCloudClientMock.execute(emptyQueryMock)).thenReturn(Collections.<AzureEntity>emptyList());

        Multimap<String, Table.Cell<String, String, String>> rowCellMap = HashMultimap.create();
        for (Table.Cell<String, String, String> cell : cells) {
            rowCellMap.put(cell.getRowKey(), cell);

            TableQuery<AzureEntity> rowValueQueryMock = mock(TableQuery.class);
            when(
                    azureTableRequestFactoryMock.containsValueForRowQuery(
                            tableName,
                            encode(cell.getRowKey()),
                            encode(cell.getValue())
                    )
            ).thenReturn(rowValueQueryMock);
            when(azureTableCloudClientMock.execute(rowValueQueryMock)).thenReturn(Collections.singletonList(ENCODE_CELL.apply(cell)));
        }

        for (Map.Entry<String, Collection<Table.Cell<String, String, String>>> entry : rowCellMap.asMap().entrySet()) {
            // row query
            TableQuery<AzureEntity> rowQueryMock = mock(TableQuery.class);
            when(azureTableRequestFactoryMock.selectAllForRow(tableName, encode(entry.getKey()))).
                    thenReturn(rowQueryMock);
            when(azureTableCloudClientMock.execute(rowQueryMock)).thenReturn(Collections2.transform(entry.getValue(), ENCODE_CELL));
        }
    }

    @SafeVarargs
    private static void setupColumnQueries(String tableName,
                                           AzureTableRequestFactory azureTableRequestFactoryMock,
                                           AzureTableCloudClient azureTableCloudClientMock,
                                           Table.Cell<String, String, String>... cells) {

        TableQuery<AzureEntity> emptyQueryMock = mock(TableQuery.class);
        when(azureTableRequestFactoryMock.selectAllForColumn(anyString(), anyString())).thenReturn(emptyQueryMock);
        when(azureTableRequestFactoryMock.containsValueForColumnQuery(anyString(), anyString(), anyString())).thenReturn(emptyQueryMock);
        when(azureTableCloudClientMock.execute(emptyQueryMock)).thenReturn(Collections.<AzureEntity>emptyList());

        Multimap<String, Table.Cell<String, String, String>> columnCellMap = HashMultimap.create();
        for (Table.Cell<String, String, String> cell : cells) {
            columnCellMap.put(cell.getColumnKey(), cell);

            TableQuery<AzureEntity> columnValueQueryMock = mock(TableQuery.class);
            when(
                    azureTableRequestFactoryMock.containsValueForColumnQuery(
                            tableName,
                            encode(cell.getColumnKey()),
                            encode(cell.getValue())
                    )
            ).thenReturn(columnValueQueryMock);
            when(azureTableCloudClientMock.execute(columnValueQueryMock)).thenReturn(Collections.singletonList(ENCODE_CELL.apply(cell)));
        }

        for (Map.Entry<String, Collection<Table.Cell<String, String, String>>> entry : columnCellMap.asMap().entrySet()) {
            // row query
            TableQuery<AzureEntity> columnQueryMock = mock(TableQuery.class);
            when(azureTableRequestFactoryMock.selectAllForColumn(tableName, encode(entry.getKey()))).
                    thenReturn(columnQueryMock);
            when(azureTableCloudClientMock.execute(columnQueryMock)).thenReturn(Collections2.transform(entry.getValue(), ENCODE_CELL));
        }
    }

}
