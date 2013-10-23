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
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class AzureTestUtil {
    private static final String ENCODING = "UTF-8";
    private static final Function<Table.Cell<String, String, String>, StringEntity> ENCODE_CELL = new Function<Table.Cell<String, String, String>, StringEntity>() {
        @Override
        public StringEntity apply(Table.Cell<String, String, String> input) {
            try {
                return encodedStringEntity(input);
            } catch (UnsupportedEncodingException e) {
                throw Throwables.propagate(e);
            }
        }
    };

    static void setAzureTableToContain(String tableName,
                                       StringTableRequestFactory stringTableRequestFactoryMock,
                                       StringTableCloudClient stringTableCloudClientMock,
                                       Table.Cell<String, String, String>... cells) throws UnsupportedEncodingException, StorageException {
        // retrieve setup in general
        TableOperation blanketRetrieveOperationMock = mock(TableOperation.class);
        when(stringTableRequestFactoryMock.retrieve(any(String.class), any(String.class))).thenReturn(blanketRetrieveOperationMock);


        // per entity setup
        Collection<StringEntity> encodedStringEntities = Lists.newArrayList();
        for (Table.Cell<String, String, String> cell : cells) {
            encodedStringEntities.add(encodedStringEntity(cell));
            setAzureTableToRetrieve(tableName, stringTableRequestFactoryMock, stringTableCloudClientMock, cell);
        }

        // select query
        TableQuery<StringEntity> tableQuery = mock(TableQuery.class);
        when(stringTableRequestFactoryMock.selectAll(tableName)).thenReturn(tableQuery);
        when(stringTableCloudClientMock.execute(tableQuery)).thenReturn(encodedStringEntities);

        setupRowQueries(tableName, stringTableRequestFactoryMock, stringTableCloudClientMock, cells);
    }

    static String encode(String stringToBeEncoded) throws UnsupportedEncodingException {
        return Base64.encode(stringToBeEncoded.getBytes(ENCODING));

    }

    static StringEntity encodedStringEntity(Table.Cell<String, String, String> unEncodedcell) throws UnsupportedEncodingException {
        return new StringEntity(encode(unEncodedcell.getRowKey()), encode(unEncodedcell.getColumnKey()), encode(unEncodedcell.getValue()));
    }

    private static void setAzureTableToRetrieve(
            String tableName,
            StringTableRequestFactory stringTableRequestFactoryMock,
            StringTableCloudClient stringTableCloudClientMock,
            Table.Cell<String, String, String> cell) throws UnsupportedEncodingException, StorageException {
        TableOperation retriveTableOperationMock = mock(TableOperation.class);
        when(stringTableRequestFactoryMock.retrieve(encode(cell.getRowKey()), encode(cell.getColumnKey()))).thenReturn(retriveTableOperationMock);
        when(stringTableCloudClientMock.execute(tableName, retriveTableOperationMock)).thenReturn(encodedStringEntity(cell));
    }

    private static void setupRowQueries(String tableName,
                                        StringTableRequestFactory stringTableRequestFactoryMock,
                                        StringTableCloudClient stringTableCloudClientMock,
                                        Table.Cell<String, String, String>... cells) {

        Multimap<String, Table.Cell<String, String, String>> rowCellMap = HashMultimap.create();
        for(Table.Cell<String, String, String> cell : cells) {
           rowCellMap.put(cell.getRowKey(), cell);
        }

        TableQuery<StringEntity> emptyQueryMock = mock(TableQuery.class);
        when(stringTableRequestFactoryMock.selectAllForRow(anyString(), anyString())).thenReturn(emptyQueryMock);
        when(stringTableCloudClientMock.execute(emptyQueryMock)).thenReturn(Collections.<StringEntity>emptyList());

        for (Map.Entry<String, Collection<Table.Cell<String, String, String>>> entry : rowCellMap.asMap().entrySet()) {
            TableQuery<StringEntity> rowQueryMock = mock(TableQuery.class);
            when(stringTableRequestFactoryMock.selectAllForRow(tableName, entry.getKey())).
                    thenReturn(rowQueryMock);
            when(stringTableCloudClientMock.execute(rowQueryMock)).thenReturn(Collections2.transform(entry.getValue(), ENCODE_CELL));
        }
    }

}
