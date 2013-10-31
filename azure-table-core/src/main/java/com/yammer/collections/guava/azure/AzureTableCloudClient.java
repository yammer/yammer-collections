package com.yammer.collections.guava.azure;

import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.table.client.CloudTableClient;
import com.microsoft.windowsazure.services.table.client.TableOperation;
import com.microsoft.windowsazure.services.table.client.TableQuery;

class AzureTableCloudClient {
    private final CloudTableClient delegate;

    AzureTableCloudClient(CloudTableClient delegateClient) {
        delegate = delegateClient;
    }

    AzureEntity execute(String tableName, TableOperation tableOperation) throws StorageException {
        return delegate.execute(tableName, tableOperation).getResultAsType();
    }

    Iterable<AzureEntity> execute(TableQuery<AzureEntity> query) {
        return delegate.execute(query);
    }
}
