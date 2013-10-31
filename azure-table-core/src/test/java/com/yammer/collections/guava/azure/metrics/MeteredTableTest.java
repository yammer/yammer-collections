package com.yammer.collections.guava.azure.metrics;


import com.google.common.collect.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MeteredTableTest {
    private static final Float ROW_KEY_1 = 0.5f;
    private static final Float ROW_KEY_2 = 0.95f;
    private static final Long COLUMN_KEY_1 = 23l;
    private static final Long COLUMN_KEY_2 = 2343l;
    private static final Integer VALUE_1 = 1;
    private static final Integer VALUE_2 = 11;
    @Mock
    private Table<Float, Long, Integer> backingTableMock;
    private Table<Float, Long, Integer> meteredTable;

    @Before
    public void setUp() {
        meteredTable = MeteredTable.create(backingTableMock);
    }

    @Test(expected = NullPointerException.class)
    public void backingTable_cannotBeNull() {
        MeteredTable.create(null);
    }

    @Test
    public void contains_delegates_to_backing_table() {
        when(backingTableMock.contains(ROW_KEY_1, COLUMN_KEY_1)).thenReturn(true);

        assertThat(meteredTable.contains(ROW_KEY_1, COLUMN_KEY_1), is(equalTo(true)));
    }

    @Test
    public void containsRow_delegates_to_backing_table() {
        when(backingTableMock.containsRow(ROW_KEY_1)).thenReturn(true);

        assertThat(meteredTable.containsRow(ROW_KEY_1), is(equalTo(true)));
    }

    @Test
    public void containsColumn_delegates_to_backing_table() {
        when(backingTableMock.containsColumn(COLUMN_KEY_1)).thenReturn(true);

        assertThat(meteredTable.containsColumn(COLUMN_KEY_1), is(equalTo(true)));
    }

    @Test
    public void containsValue_delegates_to_backing_table() {
        when(backingTableMock.containsValue(VALUE_1)).thenReturn(true);

        assertThat(meteredTable.containsValue(VALUE_1), is(equalTo(true)));
    }

    @Test
    public void get_delegates_to_backing_table() {
        when(backingTableMock.get(ROW_KEY_1, COLUMN_KEY_1)).thenReturn(VALUE_1);

        assertThat(meteredTable.get(ROW_KEY_1, COLUMN_KEY_1), is(equalTo(VALUE_1)));
    }

    public void isEmpty_delegates() {
        when(backingTableMock.isEmpty()).thenReturn(true);

        assertThat(meteredTable.isEmpty(), is(equalTo(true)));
    }

    public void size_delegates() {
        when(backingTableMock.size()).thenReturn(10);

        assertThat(meteredTable.size(), is(equalTo(10)));
    }

    public void clear_delegates() {
        meteredTable.clear();

        verify(backingTableMock).clear();
    }

    @Test
    public void put_delegates_to_backing_table() {
        when(backingTableMock.put(ROW_KEY_1, COLUMN_KEY_1, VALUE_1)).thenReturn(VALUE_1);

        assertThat(meteredTable.put(ROW_KEY_1, COLUMN_KEY_1, VALUE_1), is(equalTo(VALUE_1)));
    }

    @Test
    public void remove_delegates_to_backing_table() {
        when(backingTableMock.remove(ROW_KEY_1, COLUMN_KEY_1)).thenReturn(VALUE_1);

        assertThat(meteredTable.remove(ROW_KEY_1, COLUMN_KEY_1), is(equalTo(VALUE_1)));
    }

    public void cellSet_delegates_to_backing_table() {
        Set<Table.Cell<Float, Long, Integer>> cellSet = Collections.singleton(Tables.immutableCell(ROW_KEY_1, COLUMN_KEY_1, VALUE_1));
        when(backingTableMock.cellSet()).thenReturn(cellSet);

        Table.Cell<Float, Long, Integer> expectedCell = Tables.immutableCell(ROW_KEY_1, COLUMN_KEY_1, VALUE_1);

        //noinspection unchecked
        assertThat(meteredTable.cellSet(), containsInAnyOrder(expectedCell));
    }

    @Test
    public void columnKeySet_delegates_to_backing_table()  {
        when(backingTableMock.columnKeySet()).thenReturn(ImmutableSet.of(COLUMN_KEY_1, COLUMN_KEY_2));

        assertThat(meteredTable.columnKeySet(), containsInAnyOrder(COLUMN_KEY_1, COLUMN_KEY_2));
    }

    @Test
    public void rowKeySet_delegates_to_backing_table() {
        when(backingTableMock.rowKeySet()).thenReturn(ImmutableSet.of(ROW_KEY_1, ROW_KEY_2));

        assertThat(meteredTable.rowKeySet(), containsInAnyOrder(ROW_KEY_1, ROW_KEY_2));
    }

    @Test
    public void value_delegates_to_backing_table() throws UnsupportedEncodingException {
        when(backingTableMock.values()).thenReturn(ImmutableSet.of(VALUE_1, VALUE_2));

        assertThat(meteredTable.values(), containsInAnyOrder(VALUE_1, VALUE_2));
    }

    @Test
    public void put_all_delegates() {
        Table<Float, Long, Integer> tableToPut = new ImmutableTable.Builder<Float, Long, Integer>().
                put(ROW_KEY_1, COLUMN_KEY_1, VALUE_1).
                put(ROW_KEY_2, COLUMN_KEY_2, VALUE_2).build();

        meteredTable.putAll(tableToPut);

        verify(backingTableMock).putAll(tableToPut);
    }

    @Test
    public void rowMap_delegates() {
        Map<Float, Map<Long, Integer>> backingRowMap = ImmutableMap.of(
                ROW_KEY_1, (Map<Long, Integer>) ImmutableMap.of(COLUMN_KEY_1, VALUE_1),
                ROW_KEY_2, ImmutableMap.of(COLUMN_KEY_1, VALUE_2)
        );

        when(backingTableMock.rowMap()).thenReturn(backingRowMap);

        assertThat(meteredTable.rowMap(), is(equalTo(backingRowMap)));
    }

    @Test
    public void columnMap_delegates() {
        Map<Long, Map<Float, Integer>> backingColumnMap = ImmutableMap.of(
                COLUMN_KEY_1, (Map<Float, Integer>) ImmutableMap.of(ROW_KEY_1, VALUE_1),
                COLUMN_KEY_2, ImmutableMap.of(ROW_KEY_1, VALUE_2)
        );

        when(backingTableMock.columnMap()).thenReturn(backingColumnMap);

        assertThat(meteredTable.columnMap(), is(equalTo(backingColumnMap)));
    }

    @Test
    public void row_delegates() {
        when(backingTableMock.row(ROW_KEY_1)).thenReturn(ImmutableMap.of(COLUMN_KEY_1, VALUE_1));

        assertThat(meteredTable.row(ROW_KEY_1), is(equalTo(
                (Map<Long, Integer>) ImmutableMap.of(COLUMN_KEY_1, VALUE_1)
        )));
    }

    @Test
    public void column_delegates() {
        when(backingTableMock.column(COLUMN_KEY_1)).thenReturn(ImmutableMap.of(ROW_KEY_1, VALUE_1));

        assertThat(meteredTable.column(COLUMN_KEY_1), is(equalTo(
                (Map<Float, Integer>) ImmutableMap.of(ROW_KEY_1, VALUE_1)
        )));
    }
}
