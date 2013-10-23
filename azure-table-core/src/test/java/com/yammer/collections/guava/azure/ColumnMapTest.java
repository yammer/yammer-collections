package com.yammer.collections.guava.azure;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ColumnMapTest {
    private final static String ROW_KEY = "rowKey";
    private final static String COLUMN_KEY = "columnKey";
    private final static String VALUE = "value";
    private final static String RET_VALUE = "ret_value";

    @Mock
    private StringAzureTable stringAzureTable;
    private ColumnMap columnMap;

    @Before
    public void setUp() {
        columnMap = new ColumnMap(stringAzureTable, ROW_KEY);
    }

    @Test
    public void putDelegatesToTable() {
        when(stringAzureTable.put(ROW_KEY, COLUMN_KEY, VALUE)).thenReturn(RET_VALUE);

        assertThat(columnMap.put(COLUMN_KEY, VALUE), is(equalTo(RET_VALUE)));
    }

    @Test
    public void getDelegatesToTable() {
        when(stringAzureTable.get(ROW_KEY, COLUMN_KEY)).thenReturn(VALUE);

        assertThat(columnMap.get(COLUMN_KEY), is(equalTo(VALUE)));
    }

    @Test
    public void removeDelegatesToTable() {
        when(stringAzureTable.remove(ROW_KEY, COLUMN_KEY)).thenReturn(VALUE);

        assertThat(columnMap.remove(COLUMN_KEY), is(equalTo(VALUE)));
    }

    @Test
    public void containsKeyDelegatesToTable() {
        when(stringAzureTable.contains(ROW_KEY, COLUMN_KEY)).thenReturn(true);

        assertThat(columnMap.containsKey(COLUMN_KEY), is(equalTo(true)));
    }



}
