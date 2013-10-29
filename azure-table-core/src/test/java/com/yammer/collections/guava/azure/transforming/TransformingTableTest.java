package com.yammer.collections.guava.azure.transforming;

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
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransformingTableTest {
    private static final Float ROW_KEY_1 = 0.5f;
    private static final Float ROW_KEY_2 = 0.95f;
    private static final Long COLUMN_KEY_1 = 23l;
    private static final Long COLUMN_KEY_2 = 2343l;
    private static final Integer VALUE_1 = 1;
    private static final Integer VALUE_2 = 11;
    private static final String STRING_ROW_KEY_1 = ROW_KEY_1.toString();
    private static final String STRING_COLUMN_KEY_1 = COLUMN_KEY_1.toString();
    private static final String STRING_VALUE_1 = VALUE_1.toString();
    private static final String STRING_ROW_KEY_2 = ROW_KEY_2.toString();
    private static final String STRING_COLUMN_KEY_2 = COLUMN_KEY_2.toString();
    private static final String STRING_VALUE_2 = VALUE_2.toString();
    private TransformingTable<Float, Long, Integer, String, String, String> transformingTable;
    @Mock
    private Table<String, String, String> backingTableMock;

    @Before
    public void setUp() {
        transformingTable = new TransformingTable<>(new FloatMarshaller(), new LongMarshaller(), new IntegerMarshaller(), backingTableMock);
    }

    // contains

    @Test(expected = NullPointerException.class)
    public void when_contains_and_row_key_null_thenError() {
        transformingTable.contains(null, COLUMN_KEY_1);
    }

    @Test(expected = NullPointerException.class)
    public void when_contains_and_column_key_null_thenError() {
        transformingTable.contains(ROW_KEY_1, null);
    }

    @Test
    public void when_contains_and_row_key_of_wrong_class_then_false() {
        assertThat(transformingTable.contains(new Object(), COLUMN_KEY_1), is(equalTo(false)));
    }

    @Test
    public void when_contains_and_column_key_of_wrong_class_then_false() {
        assertThat(transformingTable.contains(ROW_KEY_1, new Object()), is(equalTo(false)));
    }

    @Test
    public void contains_delegates_to_backing_table() {
        when(backingTableMock.contains(STRING_ROW_KEY_1, STRING_COLUMN_KEY_1)).thenReturn(true);

        assertThat(transformingTable.contains(ROW_KEY_1, COLUMN_KEY_1), is(equalTo(true)));
    }

    // containsRow

    @Test(expected = NullPointerException.class)
    public void when_containsRow_and_row_key_null_thenError() {
        transformingTable.containsRow(null);
    }

    @Test
    public void when_containsRow_and_row_key_of_wrong_class_then_false() {
        assertThat(transformingTable.containsRow(new Object()), is(equalTo(false)));
    }

    @Test
    public void containsRow_delegates_to_backing_table() {
        when(backingTableMock.containsRow(STRING_ROW_KEY_1)).thenReturn(true);

        assertThat(transformingTable.containsRow(ROW_KEY_1), is(equalTo(true)));
    }

    // contains column

    @Test(expected = NullPointerException.class)
    public void when_containsColumn_and_column_key_null_thenError() {
        transformingTable.containsRow(null);
    }

    @Test
    public void when_containsColumn_and_column_key_of_wrong_class_then_false() {
        assertThat(transformingTable.containsRow(new Object()), is(equalTo(false)));
    }

    @Test
    public void containsColumn_delegates_to_backing_table() {
        when(backingTableMock.containsColumn(STRING_COLUMN_KEY_1)).thenReturn(true);

        assertThat(transformingTable.containsColumn(COLUMN_KEY_1), is(equalTo(true)));
    }


    // contains value

    @Test(expected = NullPointerException.class)
    public void when_containsValue_and_value_is_null_thenError() {
        transformingTable.containsValue(null);
    }

    @Test
    public void when_containsValue_and_value_of_wrong_class_then_false() {
        assertThat(transformingTable.containsValue(new Object()), is(equalTo(false)));
    }

    @Test
    public void containsValue_delegates_to_backing_table() {
        when(backingTableMock.containsValue(STRING_VALUE_1)).thenReturn(true);

        assertThat(transformingTable.containsValue(VALUE_1), is(equalTo(true)));
    }

    // get

    @Test(expected = NullPointerException.class)
    public void when_get_and_row_key_null_thenError() {
        transformingTable.get(null, COLUMN_KEY_1);
    }

    @Test(expected = NullPointerException.class)
    public void when_get_and_column_key_null_thenError() {
        transformingTable.get(ROW_KEY_1, null);
    }

    @Test
    public void when_get_and_row_key_of_wrong_class_then_null() {
        assertThat(transformingTable.get(new Object(), COLUMN_KEY_1), is(nullValue()));
    }

    @Test
    public void when_get_and_column_key_of_wrong_class_then_null() {
        assertThat(transformingTable.get(ROW_KEY_1, new Object()), is(nullValue()));
    }

    @Test
    public void get_delegates_to_backing_table() {
        when(backingTableMock.get(STRING_ROW_KEY_1, STRING_COLUMN_KEY_1)).thenReturn(STRING_VALUE_1);

        assertThat(transformingTable.get(ROW_KEY_1, COLUMN_KEY_1), is(equalTo(VALUE_1)));
    }

    // isEmpty

    public void isEmpty_delegates() {
        when(backingTableMock.isEmpty()).thenReturn(true);

        assertThat(transformingTable.isEmpty(), is(equalTo(true)));
    }

    // size

    public void size_delegates() {
        when(backingTableMock.size()).thenReturn(10);

        assertThat(transformingTable.size(), is(equalTo(10)));
    }

    // clear

    public void clear_delegates() {
        transformingTable.clear();

        verify(backingTableMock).clear();
    }

    // put

    @Test(expected = NullPointerException.class)
    public void when_put_and_row_key_null_thenError() {
        transformingTable.put(null, COLUMN_KEY_1, VALUE_1);
    }

    @Test(expected = NullPointerException.class)
    public void when_put_and_column_key_null_thenError() {
        transformingTable.put(ROW_KEY_1, null, VALUE_1);
    }

    @Test(expected = NullPointerException.class)
    public void when_put_and_value_null_thenError() {
        transformingTable.put(ROW_KEY_1, COLUMN_KEY_1, null);
    }

    @Test
    public void put_delegates_to_backing_table() {
        when(backingTableMock.put(STRING_ROW_KEY_1, STRING_COLUMN_KEY_1, STRING_VALUE_1)).thenReturn(STRING_VALUE_1);

        assertThat(transformingTable.put(ROW_KEY_1, COLUMN_KEY_1, VALUE_1), is(equalTo(VALUE_1)));
    }

    // remove

    @Test(expected = NullPointerException.class)
    public void when_remove_and_row_key_null_thenError() {
        transformingTable.remove(null, COLUMN_KEY_1);
    }

    @Test(expected = NullPointerException.class)
    public void when_remove_and_column_key_null_thenError() {
        transformingTable.remove(ROW_KEY_1, null);
    }

    @Test
    public void when_remove_and_row_key_of_wrong_class_then_null() {
        assertThat(transformingTable.remove(new Object(), COLUMN_KEY_1), is(nullValue()));
    }

    @Test
    public void when_remove_and_column_key_of_wrong_class_then_null() {
        assertThat(transformingTable.remove(ROW_KEY_1, new Object()), is(nullValue()));
    }

    @Test
    public void remove_delegates_to_backing_table() {
        when(backingTableMock.remove(STRING_ROW_KEY_1, STRING_COLUMN_KEY_1)).thenReturn(STRING_VALUE_1);

        assertThat(transformingTable.remove(ROW_KEY_1, COLUMN_KEY_1), is(equalTo(VALUE_1)));
    }

    // cell set
    public void cellSet_delegates_to_backing_table() {
        Set<Table.Cell<String, String, String>> cellSet = Collections.singleton(Tables.immutableCell(STRING_ROW_KEY_1, STRING_COLUMN_KEY_1, STRING_VALUE_1));
        when(backingTableMock.cellSet()).thenReturn(cellSet);

        Table.Cell<Float, Long, Integer> expectedCell = Tables.immutableCell(ROW_KEY_1, COLUMN_KEY_1, VALUE_1);

        //noinspection unchecked
        assertThat(transformingTable.cellSet(), containsInAnyOrder(expectedCell));
    }

    // column key set

    @Test
    public void columnKeySet_delegates_to_backing_table() throws UnsupportedEncodingException {
        when(backingTableMock.columnKeySet()).thenReturn(ImmutableSet.of(STRING_COLUMN_KEY_1, STRING_COLUMN_KEY_2));

        assertThat(transformingTable.columnKeySet(), containsInAnyOrder(COLUMN_KEY_1, COLUMN_KEY_2));
    }

    // row key set

    @Test
    public void rowKeySet_delegates_to_backing_table() throws UnsupportedEncodingException {
        when(backingTableMock.rowKeySet()).thenReturn(ImmutableSet.of(STRING_ROW_KEY_1, STRING_ROW_KEY_2));

        assertThat(transformingTable.rowKeySet(), containsInAnyOrder(ROW_KEY_1, ROW_KEY_2));
    }

    // values

    @Test
    public void value_delegates_to_backing_table() throws UnsupportedEncodingException {
        when(backingTableMock.values()).thenReturn(ImmutableSet.of(STRING_VALUE_1, STRING_VALUE_2));

        assertThat(transformingTable.values(), containsInAnyOrder(VALUE_1, VALUE_2));
    }

    // put all delegates
    @Test
    public void put_all_delegates() {
        Table<Float, Long, Integer> tableToPut = new ImmutableTable.Builder().
                put(ROW_KEY_1, COLUMN_KEY_1, VALUE_1).
                put(ROW_KEY_2, COLUMN_KEY_2, VALUE_2).build();

        transformingTable.putAll(tableToPut);

        verify(backingTableMock).put(STRING_ROW_KEY_1, STRING_COLUMN_KEY_1, STRING_VALUE_1);
        verify(backingTableMock).put(STRING_ROW_KEY_2, STRING_COLUMN_KEY_2, STRING_VALUE_2);
    }

    @Test(expected = NullPointerException.class)
    public void put_all_errors_on_null() {
        transformingTable.putAll(null);
    }

    // row map
    @Test
    public void rowMap_delegates() {
        Map<String, Map<String, String>> backingRowMap = ImmutableMap.of(
                STRING_ROW_KEY_1, (Map<String, String>) ImmutableMap.of(STRING_COLUMN_KEY_1, STRING_VALUE_1),
                STRING_ROW_KEY_2, ImmutableMap.of(STRING_COLUMN_KEY_1, STRING_VALUE_2)
        );

        when(backingTableMock.rowMap()).thenReturn(backingRowMap);

        assertThat(transformingTable.rowMap(), is(equalTo(
                (Map<Float, Map<Long, Integer>>) ImmutableMap.of(
                        ROW_KEY_1, (Map<Long, Integer>) ImmutableMap.of(COLUMN_KEY_1, VALUE_1),
                        ROW_KEY_2, ImmutableMap.of(COLUMN_KEY_1, VALUE_2))
        )));
    }

    // column map
    @Test
    public void columnMap_delegates() {
        Map<String, Map<String, String>> backingRowMap = ImmutableMap.of(
                STRING_COLUMN_KEY_1, (Map<String, String>) ImmutableMap.of(STRING_ROW_KEY_1, STRING_VALUE_1),
                STRING_COLUMN_KEY_2, ImmutableMap.of(STRING_ROW_KEY_1, STRING_VALUE_2)
        );

        when(backingTableMock.columnMap()).thenReturn(backingRowMap);

        assertThat(transformingTable.columnMap(), is(equalTo(
                (Map<Long, Map<Float, Integer>>) ImmutableMap.of(
                        COLUMN_KEY_1, (Map<Float, Integer>) ImmutableMap.of(ROW_KEY_1, VALUE_1),
                        COLUMN_KEY_2, ImmutableMap.of(ROW_KEY_1, VALUE_2))
        )));
    }

    @Test
    public void row_delegates() {
        when(backingTableMock.row(STRING_ROW_KEY_1)).thenReturn(ImmutableMap.of(STRING_COLUMN_KEY_1, STRING_VALUE_1));

        assertThat(transformingTable.row(ROW_KEY_1), is(equalTo(
                (Map<Long, Integer>) ImmutableMap.of(COLUMN_KEY_1, VALUE_1)
        )));
    }

    @Test(expected = NullPointerException.class)
    public void when_row_null_then_error() {
        transformingTable.row(null);
    }

    @Test
    public void column_delegates() {
        when(backingTableMock.column(STRING_COLUMN_KEY_1)).thenReturn(ImmutableMap.of(STRING_ROW_KEY_1, STRING_VALUE_1));

        assertThat(transformingTable.column(COLUMN_KEY_1), is(equalTo(
                (Map<Float, Integer>) ImmutableMap.of(ROW_KEY_1, VALUE_1)
        )));
    }

    @Test(expected = NullPointerException.class)
    public void when_column_null_then_error() {
        transformingTable.column(null);
    }

    // ----- stubs ----

    private class FloatMarshaller implements TransformingTable.Marshaller<Float, String> {
        @Override
        public String marshal(Float unmarshalled) {
            return unmarshalled.toString();
        }

        @Override
        public Float unmarshal(String marshalled) {
            return Float.parseFloat(marshalled);
        }

        @Override
        public Class<Float> getType() {
            return Float.class;
        }
    }

    private class LongMarshaller implements TransformingTable.Marshaller<Long, String> {

        @Override
        public String marshal(Long unmarshalled) {
            return unmarshalled.toString();
        }

        @Override
        public Long unmarshal(String marshalled) {
            return Long.parseLong(marshalled);
        }

        @Override
        public Class<Long> getType() {
            return Long.class;
        }
    }

    private class IntegerMarshaller implements TransformingTable.Marshaller<Integer, String> {
        @Override
        public String marshal(Integer unmarshalled) {
            return unmarshalled.toString();
        }

        @Override
        public Integer unmarshal(String marshalled) {
            return Integer.parseInt(marshalled);
        }

        @Override
        public Class<Integer> getType() {
            return Integer.class;
        }
    }
}
