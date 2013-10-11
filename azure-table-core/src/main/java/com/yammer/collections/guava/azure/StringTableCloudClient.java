package com.yammer.collections.guava.azure;

import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.TableOperation;
import com.microsoft.windowsazure.services.table.client.TableQuery;

/*package*/ class StringTableCloudClient {
    private final CloudTableClient delegate;

    StringTableCloudClient(CloudTableClient delegateClient) {
        delegate = delegateClient;
    }

    StringEntity execute(String tableName, TableOperation tableOperation) throws StorageException {
        return delegate.execute(tableName, tableOperation).getResultAsType();
    }

    Iterable<StringEntity> execute(TableQuery<StringEntity> query) {
        return delegate.execute(query);
    }
}
