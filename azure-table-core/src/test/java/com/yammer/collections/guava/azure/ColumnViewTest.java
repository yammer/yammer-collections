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

import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"InstanceVariableMayNotBeInitialized", "SuspiciousMethodCalls"})
@RunWith(MockitoJUnitRunner.class)
public class ColumnViewTest {
    private static final String ROW_KEY = "rowKey";
    private static final String COLUMN_KEY_1 = "columnKey1";
    private static final String VALUE_1 = "value1";
    private static final String COLUMN_KEY_2 = "columnKey2";
    private static final String VALUE_2 = "value2";
    private static final String RET_VALUE = "ret_value";
    private static final String OTHER_ROW_KEY = "otherRow";
    private static final String OTHER_COLUMN_KEY = "otherKey";
    private static final String OTHER_VALUE = "otherValue";
    private static final String TABLE_NAME = "secretie_table";
    private static final Table.Cell<String, String, String> CELL_1 = Tables.immutableCell(ROW_KEY, COLUMN_KEY_1, VALUE_1);
    private static final Table.Cell<String, String, String> CELL_2 = Tables.immutableCell(ROW_KEY, COLUMN_KEY_2, VALUE_2);
    private static final Table.Cell<String, String, String> CELL_WITH_OTHER_ROW_KEY = Tables.immutableCell(OTHER_ROW_KEY, OTHER_COLUMN_KEY, OTHER_VALUE);
    private static final Function<Map.Entry, TestMapEntry> MAP_TO_ENTRIES = new Function<Map.Entry, TestMapEntry>() {
        @SuppressWarnings("ClassEscapesDefinedScope")
        @Override
        public TestMapEntry apply(Map.Entry input) {
            return new TestMapEntry(input);
        }
    };
    @Mock
    private AzureTableCloudClient azureTableCloudClientMock;
    @Mock
    private AzureTableRequestFactory azureTableRequestFactoryMock;
    @Mock
    private BaseAzureTable baseAzureTable;
    private ColumnView columnView;

    @Before
    public void setUp() {
        when(baseAzureTable.getTableName()).thenReturn(TABLE_NAME);
        columnView = new ColumnView(baseAzureTable, ROW_KEY, azureTableCloudClientMock, azureTableRequestFactoryMock);
    }

    @Test
    public void put_delegates_to_table() {
        when(baseAzureTable.put(ROW_KEY, COLUMN_KEY_1, VALUE_1)).thenReturn(RET_VALUE);

        assertThat(columnView.put(COLUMN_KEY_1, VALUE_1), is(equalTo(RET_VALUE)));
    }

    @Test
    public void get_delegates_to_table() {
        when(baseAzureTable.get(ROW_KEY, COLUMN_KEY_1)).thenReturn(VALUE_1);

        assertThat(columnView.get(COLUMN_KEY_1), is(equalTo(VALUE_1)));
    }

    @Test
    public void remove_delegates_to_table() {
        when(baseAzureTable.remove(ROW_KEY, COLUMN_KEY_1)).thenReturn(VALUE_1);

        assertThat(columnView.remove(COLUMN_KEY_1), is(equalTo(VALUE_1)));
    }

    @Test
    public void contains_key_delegates_to_table() {
        when(baseAzureTable.contains(ROW_KEY, COLUMN_KEY_1)).thenReturn(true);

        assertThat(columnView.containsKey(COLUMN_KEY_1), is(equalTo(true)));
    }

    @Test
    public void putAll_delegates_to_table() {
        columnView.putAll(
                ImmutableMap.of(
                        COLUMN_KEY_1, VALUE_1,
                        COLUMN_KEY_2, VALUE_2
                ));

        verify(baseAzureTable).put(ROW_KEY, COLUMN_KEY_1, VALUE_1);
        verify(baseAzureTable).put(ROW_KEY, COLUMN_KEY_2, VALUE_2);
    }

    @Test
    public void keySet_returns_contained_keys() throws StorageException {
        setAzureTableToContain(CELL_1, CELL_2, CELL_WITH_OTHER_ROW_KEY);

        assertThat(columnView.keySet(), containsInAnyOrder(COLUMN_KEY_1, COLUMN_KEY_2));
    }

    @Test
    public void values_returns_contained_values() throws StorageException {
        setAzureTableToContain(CELL_1, CELL_2, CELL_WITH_OTHER_ROW_KEY);

        assertThat(columnView.values(), containsInAnyOrder(VALUE_1, VALUE_2));
    }

    @Test
    public void entrySet_returns_contained_entries() throws StorageException {
        setAzureTableToContain(CELL_1, CELL_2, CELL_WITH_OTHER_ROW_KEY);

        assertThat(
                Iterables.transform(columnView.entrySet(), MAP_TO_ENTRIES),
                containsInAnyOrder(
                        new TestMapEntry(COLUMN_KEY_1, VALUE_1),
                        new TestMapEntry(COLUMN_KEY_2, VALUE_2)
                ));
    }

    @Test
    public void setValue_on_entry_updates_backing_table() throws StorageException {
        setAzureTableToContain(CELL_1, CELL_2, CELL_WITH_OTHER_ROW_KEY);
        when(baseAzureTable.put(ROW_KEY, COLUMN_KEY_1, OTHER_VALUE)).thenReturn(RET_VALUE);
        when(baseAzureTable.put(ROW_KEY, COLUMN_KEY_2, OTHER_VALUE)).thenReturn(RET_VALUE);

        Map.Entry<String, String> someEntry = columnView.entrySet().iterator().next();

        assertThat(someEntry.setValue(OTHER_VALUE), is(equalTo(RET_VALUE)));
        verify(baseAzureTable).put(ROW_KEY, someEntry.getKey(), OTHER_VALUE);
    }

    @Test
    public void size_returns_correct_value() throws StorageException {
        setAzureTableToContain(CELL_1, CELL_2, CELL_WITH_OTHER_ROW_KEY);

        assertThat(columnView.size(), is(equalTo(2)));
    }

    @Test
    public void clear_deletes_values_from_key_set() throws StorageException {
        setAzureTableToContain(CELL_1, CELL_2, CELL_WITH_OTHER_ROW_KEY);

        columnView.clear();

        verify(baseAzureTable).remove(ROW_KEY, COLUMN_KEY_1);
        verify(baseAzureTable).remove(ROW_KEY, COLUMN_KEY_2);
    }

    @Test
    public void isEmpty_returns_false_if_no_entires() throws StorageException {
        setAzureTableToContain(CELL_WITH_OTHER_ROW_KEY);

        assertThat(columnView.isEmpty(), is(equalTo(true)));
    }

    @Test
    public void isEmpty_returns_true_if_there_are_entires() throws StorageException {
        setAzureTableToContain(CELL_1);

        assertThat(columnView.isEmpty(), is(equalTo(false)));
    }

    @Test
    public void contains_value_returns_true_if_value_contains() throws StorageException {
        setAzureTableToContain(CELL_1, CELL_WITH_OTHER_ROW_KEY);

        assertThat(columnView.containsValue(VALUE_1), is(equalTo(true)));
    }

    @Test
    public void contains_value_returns_false_if_does_not_contain_value_in_row() throws StorageException {
        setAzureTableToContain(Tables.immutableCell(OTHER_ROW_KEY, COLUMN_KEY_1, VALUE_1));

        assertThat(columnView.containsValue(VALUE_1), is(equalTo(false)));
    }

    @Test
    public void contains_value_returns_false_if_object_not_string() throws StorageException {
        setAzureTableToContain();

        assertThat(columnView.containsValue(new Object()), is(equalTo(false)));
    }


    //----------------------
    // Utilities
    //----------------------

    @SafeVarargs
    private final void setAzureTableToContain(Table.Cell<String, String, String>... cells) throws StorageException {
        for (Table.Cell<String, String, String> cell : cells) {
            when(baseAzureTable.get(cell.getRowKey(), cell.getColumnKey())).thenReturn(cell.getValue());
        }
        AzureTestUtil.setAzureTableToContain(TABLE_NAME, azureTableRequestFactoryMock, azureTableCloudClientMock, cells);
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
