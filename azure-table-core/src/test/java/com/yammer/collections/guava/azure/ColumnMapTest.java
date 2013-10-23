package com.yammer.collections.guava.azure;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.UnsupportedEncodingException;
import java.util.Map;

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
    private static final Table.Cell<String, String, String> CELL_WITH_OTHER_ROW_KEY = Tables.immutableCell(OTHER_ROW_KEY, OTHER_COLUMN_KEY, OTHER_VALUE);
    private static final Function<Map.Entry, TestMapEntry> MAP_TO_ENTRIES = new Function<Map.Entry, TestMapEntry>() {
        @Override
        public TestMapEntry apply(Map.Entry input) {
            return new TestMapEntry(input);
        }
    };
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
        setAzureTableToContain(CELL_1, CELL_2, CELL_WITH_OTHER_ROW_KEY);

        assertThat(columnMap.keySet(), containsInAnyOrder(COLUMN_KEY_1, COLUMN_KEY_2));
    }

    @Test
    public void values_returns_contained_values() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1, CELL_2, CELL_WITH_OTHER_ROW_KEY);

        assertThat(columnMap.values(), containsInAnyOrder(VALUE_1, VALUE_2));
    }

    @Test
    public void entrySet_returns_contained_entries() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1, CELL_2, CELL_WITH_OTHER_ROW_KEY);

        assertThat(
                Iterables.transform(columnMap.entrySet(), MAP_TO_ENTRIES),
                containsInAnyOrder(
                        new TestMapEntry(COLUMN_KEY_1, VALUE_1),
                        new TestMapEntry(COLUMN_KEY_2, VALUE_2)
                ));
    }

    @Test
    public void setValue_on_entry_updates_backing_table() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1, CELL_2, CELL_WITH_OTHER_ROW_KEY);
        when(stringAzureTable.put(ROW_KEY, COLUMN_KEY_1, OTHER_VALUE)).thenReturn(RET_VALUE);
        when(stringAzureTable.put(ROW_KEY, COLUMN_KEY_2, OTHER_VALUE)).thenReturn(RET_VALUE);

        Map.Entry<String, String> someEntry = columnMap.entrySet().iterator().next();

        assertThat(someEntry.setValue(OTHER_VALUE), is(equalTo(RET_VALUE)));
        verify(stringAzureTable).put(ROW_KEY, someEntry.getKey(), OTHER_VALUE);
    }

    @Test
    public void size_returns_correct_value() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1, CELL_2, CELL_WITH_OTHER_ROW_KEY);

        assertThat(columnMap.size(), is(equalTo(2)));
    }

    @Test
    public void clear_deletes_values_from_key_set() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1, CELL_2, CELL_WITH_OTHER_ROW_KEY);

        columnMap.clear();

        verify(stringAzureTable).remove(ROW_KEY, COLUMN_KEY_1);
        verify(stringAzureTable).remove(ROW_KEY, COLUMN_KEY_2);
    }

    @Test
    public void isEmpty_returns_false_if_no_entires() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_WITH_OTHER_ROW_KEY);

        assertThat(columnMap.isEmpty(), is(equalTo(true)));
    }

    @Test
    public void isEmpty_returns_true_if_there_are_entires() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1);

        assertThat(columnMap.isEmpty(), is(equalTo(false)));
    }

    @Test
    public void contains_value_returns_true_if_value_contains() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1, CELL_WITH_OTHER_ROW_KEY);

        assertThat(columnMap.containsValue(VALUE_1), is(equalTo(true)));
    }

    @Test
    public void contains_value_returns_false_if_does_not_contain_value_in_row() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(Tables.immutableCell(OTHER_ROW_KEY, COLUMN_KEY_1, VALUE_1));

        assertThat(columnMap.containsValue(VALUE_1), is(equalTo(false)));
    }



    //----------------------
    // Utilities
    //----------------------

    private void setAzureTableToContain(Table.Cell<String, String, String>... cells) throws UnsupportedEncodingException, StorageException {
        for(Table.Cell<String, String, String> cell : cells) {
            when(stringAzureTable.get(cell.getRowKey(), cell.getColumnKey())).thenReturn(cell.getValue());
        }
        AzureTestUtil.setAzureTableToContain(TABLE_NAME, stringTableRequestFactoryMock, stringTableCloudClientMock, cells);
    }

    private static class TestMapEntry implements Map.Entry<String, String> {
        private final String key;
        private final String value;

        public TestMapEntry(Map.Entry<String, String> entry) {
            this(entry.getKey(), entry.getValue());
        }

        public TestMapEntry(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String setValue(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestMapEntry that = (TestMapEntry) o;

            if (key != null ? !key.equals(that.key) : that.key != null) return false;
            if (value != null ? !value.equals(that.value) : that.value != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "[" + key + "," + value + "]";
        }
    }


}
