package com.yammer.collections.guava.azure;

import com.microsoft.windowsazure.services.table.client.TableConstants;
import com.microsoft.windowsazure.services.table.client.TableOperation;
import com.microsoft.windowsazure.services.table.client.TableQuery;

import java.util.Set;

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

    TableQuery<StringEntity> selectAllForRow(String tableName, String rowKey) {
        String partitionFilter = TableQuery.generateFilterCondition(
                TableConstants.PARTITION_KEY,
                TableQuery.QueryComparisons.EQUAL,
                rowKey);

        return selectAll(tableName).where(partitionFilter);
    }
}
