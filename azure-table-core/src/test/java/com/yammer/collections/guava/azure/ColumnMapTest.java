package com.yammer.collections.guava.azure;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ColumnMapTest {
    private final static String ROW_KEY = "rowKey";
    private final static String COLUMN_KEY_1 = "columnKey1";
    private final static String VALUE_1 = "value1";
    private final static String COLUMN_KEY_2 = "columnKey2";
    private final static String VALUE_2 = "value2";
    private final static String RET_VALUE = "ret_value";
    @Mock
    private StringAzureTable stringAzureTable;
    private ColumnMap columnMap;

    @Before
    public void setUp() {
        columnMap = new ColumnMap(stringAzureTable, ROW_KEY);
    }

    @Test
    public void put_delegates_to_table() {
        when(stringAzureTable.put(ROW_KEY, COLUMN_KEY_1, VALUE_1)).thenReturn(RET_VALUE);

        assertThat(columnMap.put(COLUMN_KEY_1, VALUE_1), is(equalTo(RET_VALUE)));
    }

    @Test
    public void get_delegates_to_table() {
        when(stringAzureTable.get(ROW_KEY, COLUMN_KEY_1)).thenReturn(VALUE_1);

        assertThat(columnMap.get(COLUMN_KEY_1), is(equalTo(VALUE_1)));
    }

    @Test
    public void removeDelegatesToTable() {
        when(stringAzureTable.remove(ROW_KEY, COLUMN_KEY_1)).thenReturn(VALUE_1);

        assertThat(columnMap.remove(COLUMN_KEY_1), is(equalTo(VALUE_1)));
    }

    @Test
    public void containsKeyDelegatesToTable() {
        when(stringAzureTable.contains(ROW_KEY, COLUMN_KEY_1)).thenReturn(true);

        assertThat(columnMap.containsKey(COLUMN_KEY_1), is(equalTo(true)));
    }

    @Test
    public void putAllDelegatesToTable() {
        columnMap.putAll(
                ImmutableMap.of(
                        COLUMN_KEY_1, VALUE_1,
                        COLUMN_KEY_2, VALUE_2
                ));

        verify(stringAzureTable).put(ROW_KEY, COLUMN_KEY_1, VALUE_1);
        verify(stringAzureTable).put(ROW_KEY, COLUMN_KEY_2, VALUE_2);
    }


}
