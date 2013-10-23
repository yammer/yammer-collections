package com.yammer.collections.guava.azure;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.UnsupportedEncodingException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
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
    private final static String OTHER_ROW_KEY = "otherRow";
    private final static String OTHER_COLUMN_KEY = "otherKey";
    private final static String OTHER_VALUE = "otherValue";
    private static final String TABLE_NAME = "secretie_table";
    private static final Table.Cell<String, String, String> CELL_1 = Tables.immutableCell(ROW_KEY, COLUMN_KEY_1, VALUE_1);
    private static final Table.Cell<String, String, String> CELL_2 = Tables.immutableCell(ROW_KEY, COLUMN_KEY_2, VALUE_2);
    private static final Table.Cell<String, String, String> OTHER_CELL = Tables.immutableCell(OTHER_ROW_KEY, OTHER_COLUMN_KEY, OTHER_VALUE);

    @Mock
    private StringTableCloudClient stringTableCloudClientMock;
    @Mock
    private StringTableRequestFactory stringTableRequestFactoryMock;
    @Mock
    private StringAzureTable stringAzureTable;
    private ColumnMap columnMap;

    @Before
    public void setUp() {
        when(stringAzureTable.getTableName()).thenReturn(TABLE_NAME);
        columnMap = new ColumnMap(stringAzureTable, ROW_KEY, stringTableCloudClientMock, stringTableRequestFactoryMock);
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
    public void remove_delegates_to_table() {
        when(stringAzureTable.remove(ROW_KEY, COLUMN_KEY_1)).thenReturn(VALUE_1);

        assertThat(columnMap.remove(COLUMN_KEY_1), is(equalTo(VALUE_1)));
    }

    @Test
    public void contains_key_delegates_to_table() {
        when(stringAzureTable.contains(ROW_KEY, COLUMN_KEY_1)).thenReturn(true);

        assertThat(columnMap.containsKey(COLUMN_KEY_1), is(equalTo(true)));
    }

    @Test
    public void putAll_delegates_to_table() {
        columnMap.putAll(
                ImmutableMap.of(
                        COLUMN_KEY_1, VALUE_1,
                        COLUMN_KEY_2, VALUE_2
                ));

        verify(stringAzureTable).put(ROW_KEY, COLUMN_KEY_1, VALUE_1);
        verify(stringAzureTable).put(ROW_KEY, COLUMN_KEY_2, VALUE_2);
    }

    @Test
    public void keySet_returns_contained_keys() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1, CELL_2, OTHER_CELL);

        assertThat(columnMap.keySet(), containsInAnyOrder(COLUMN_KEY_1, COLUMN_KEY_2));
    }

    private void setAzureTableToContain(Table.Cell<String, String, String>... cells) throws UnsupportedEncodingException, StorageException {
        AzureTestUtil.setAzureTableToContain(TABLE_NAME, stringTableRequestFactoryMock, stringTableCloudClientMock, cells);
    }


}
