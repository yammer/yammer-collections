package com.yammer.collections.guava.azure;

import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CellSetViewTest {
    private static final String ROW_KEY_1 = "rown_name_1";
    private static final String ROW_KEY_2 = "row_name_2";
    private static final String COLUMN_KEY_1 = "column_key_1";
    private static final String COLUMN_KEY_2 = "column_key_2";
    private static final String VALUE_1 = "value1";
    private static final String VALUE_2 = "value3";
    private static final String TABLE_NAME = "secretie_table";
    private static final Table.Cell<String, String, String> CELL_1 = Tables.immutableCell(ROW_KEY_1, COLUMN_KEY_1, VALUE_1);
    private static final Table.Cell<String, String, String> CELL_2 = Tables.immutableCell(ROW_KEY_2, COLUMN_KEY_2, VALUE_2);

    @Mock
    private StringAzureTable stringAzureTable;
    @Mock
    private StringTableCloudClient stringTableCloudClientMock;
    @Mock
    private StringTableRequestFactory stringTableRequestFactoryMock;

    private CellSetView set;

    @Before
    public void setUp() {
        when(stringAzureTable.getTableName()).thenReturn(TABLE_NAME);
        set = new CellSetView(stringAzureTable, stringTableCloudClientMock, stringTableRequestFactoryMock);
    }

    @Test
    public void size_returns_correct_value() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1, CELL_2);

        assertThat(set.size(), is(equalTo(2)));
    }

    @Test
    public void is_returns_false_on_non_empty_set() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1);

        assertThat(set.isEmpty(), is(equalTo(false)));
    }

    @Test
    public void is_returns_true_on_empty_set() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain();

        assertThat(set.isEmpty(), is(equalTo(true)));
    }

    @Test
    public void contains_on_non_table_cell_returns_false() {
        assertThat(set.contains(new Object()), is(equalTo(false)));
    }

    @Test
    public void contains_delegates_to_table() {
        Object o1 = new Object();
        Object o2 = new Object();
        Table.Cell<Object, Object, Object> cell = Tables.immutableCell(o1, o2, new Object());
        when(stringAzureTable.contains(o1, o2)).thenReturn(true);

        assertThat(set.contains(cell), is(equalTo(true)));
    }

    @Test
    public void iterator_contains_contained_entities() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1, CELL_2);

        assertThat(set, containsInAnyOrder(CELL_1, CELL_2));
    }

    @Test
    public void add_delegates_to_table() {
        set.add(CELL_1);

        verify(stringAzureTable).put(ROW_KEY_1, COLUMN_KEY_1, VALUE_1);
    }

    @Test
    public void when_value_existed_in_table_then_add_returns_false() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1);

        assertThat(set.add(CELL_1), is(equalTo(false)));
    }

    @Test
    public void when_value_did_not_exist_in_table_then_add_returns_true() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain();

        assertThat(set.add(CELL_1), is(equalTo(true)));
    }

    @Test
    public void remove_delegates_to_table() {
        set.remove(CELL_1);

        verify(stringAzureTable).remove(ROW_KEY_1, COLUMN_KEY_1);
    }

    @Test
    public void when_value_existed_in_table_then_remove_returns_true() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1);

        assertThat(set.remove(CELL_1), is(equalTo(true)));
    }

    @Test
    public void when_value_did_not_exist_in_table_then_remove_returns_false() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain();

        assertThat(set.remove(CELL_1), is(equalTo(false)));
    }

    @Test
    public void when_object_to_be_removed_is_not_a_table_cell_then_remove_returns_false() {
        assertThat(set.remove(new Object()), is(equalTo(false)));
    }

    @Test
    public void when_contains_all_then_contains_all_returns_true() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1, CELL_2);

        assertThat(set.containsAll(Arrays.asList(CELL_1, CELL_2)), is(equalTo(true)));

    }

    @Test
    public void when_does_not_contain_all_then_returns_false() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_2);

        assertThat(set.containsAll(Arrays.asList(CELL_1, CELL_2)), is(equalTo(false)));
    }

    @Test
    public void add_all_adds_to_table() {
        set.addAll(Arrays.asList(CELL_1, CELL_2));

        verify(stringAzureTable).put(ROW_KEY_1, COLUMN_KEY_1, VALUE_1);
        verify(stringAzureTable).put(ROW_KEY_2, COLUMN_KEY_2, VALUE_2);
    }


    @Test
    public void when_all_values_where_contained_then_return_false() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1, CELL_2);

        assertThat(set.addAll(Arrays.asList(CELL_1, CELL_2)), is(equalTo(false)));
    }

    @Test
    public void when_some_value_was_not_present_then_return_true() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1);

        assertThat(set.addAll(Arrays.asList(CELL_1, CELL_2)), is(equalTo(true)));
    }

    @Test
    public void remove_all_removes_from_table() {
        set.removeAll(Arrays.asList(CELL_1, CELL_2));

        verify(stringAzureTable).remove(ROW_KEY_1, COLUMN_KEY_1);
        verify(stringAzureTable).remove(ROW_KEY_2, COLUMN_KEY_2);
    }


    @Test
    public void when_some_remove_all_values_where_contained_then_return_true() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1, CELL_2);

        assertThat(set.removeAll(Arrays.asList(CELL_1, CELL_2)), is(equalTo(true)));
    }

    @Test
    public void when_non_of_remove_all_values_existed_then_return_false() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain();

        assertThat(set.removeAll(Arrays.asList(CELL_1, CELL_2)), is(equalTo(false)));
    }

    @Test
    public void clear_deletes_contained_entries() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1, CELL_2);

        set.clear();

        verify(stringAzureTable).remove(ROW_KEY_1, COLUMN_KEY_1);
        verify(stringAzureTable).remove(ROW_KEY_2, COLUMN_KEY_2);
    }

    //----------------------
    // Utilities
    //----------------------

    private void setAzureTableToContain(Table.Cell<String, String, String>... cells) throws UnsupportedEncodingException, StorageException {
        for(Table.Cell<String, String, String> cell : cells) {
            when(stringAzureTable.get(cell.getRowKey(), cell.getColumnKey())).thenReturn(cell.getValue());
            when(stringAzureTable.contains(cell.getRowKey(), cell.getColumnKey())).thenReturn(true);
            when(stringAzureTable.put(eq(cell.getRowKey()), eq(cell.getColumnKey()), anyString())).thenReturn(cell.getValue());
            when(stringAzureTable.remove(cell.getRowKey(), cell.getColumnKey())).thenReturn(cell.getValue());

        }
        AzureTestUtil.setAzureTableToContain(TABLE_NAME, stringTableRequestFactoryMock, stringTableCloudClientMock, cells);
    }


}
