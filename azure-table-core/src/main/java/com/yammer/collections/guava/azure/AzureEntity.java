package com.yammer.collections.guava.azure;

import com.microsoft.windowsazure.services.table.client.TableServiceEntity;

public class AzureEntity extends TableServiceEntity {
    public static final String VALUE = "Value";
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    private String value; // cannot be final

    public AzureEntity() { // needed by azure java api
    }

    public AzureEntity(String rowKey, String columnKey, String value) {
        partitionKey = rowKey;
        this.rowKey = columnKey;
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}