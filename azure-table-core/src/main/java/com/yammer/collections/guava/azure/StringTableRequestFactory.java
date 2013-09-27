package com.yammer.collections.guava.azure;

import com.microsoft.windowsazure.services.table.client.TableOperation;
import com.microsoft.windowsazure.services.table.client.TableQuery;

/* pacakge */class StringTableRequestFactory {
    public TableOperation put(String rowString, String columnString, String value) {
        StringEntity secretieEntity = new StringEntity(rowString, columnString, value);
        return TableOperation.insertOrReplace(secretieEntity);
    }

    TableOperation retrieve(String row, String column) {
        return TableOperation.retrieve(row, column, StringEntity.class);
    }

    TableOperation delete(StringEntity entityToBeDeleted) {
        return TableOperation.delete(entityToBeDeleted);
    }

    TableQuery<StringEntity> selectAll(String tableName) {
        return TableQuery.from(tableName, StringEntity.class);
    }
}
