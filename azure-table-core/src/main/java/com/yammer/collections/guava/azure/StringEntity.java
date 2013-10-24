package com.yammer.collections.guava.azure;

import com.microsoft.windowsazure.services.table.client.TableServiceEntity;

public class StringEntity extends TableServiceEntity {
    public static final String VALUE = "Value";
    private String value;

    public StringEntity() { // needed by azure java api
    }

    public StringEntity(String rowKey, String columnKey, String value) {
        this.partitionKey = rowKey;
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