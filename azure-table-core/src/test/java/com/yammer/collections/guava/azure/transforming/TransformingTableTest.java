package com.yammer.collections.guava.azure.transforming;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
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
    private Table<String, String, String> baseTableMock;

    @Before
    public void setUp() {
        transformingTable = new TransformingTable<>(new FloatMarshaller(), new LongMarshaller(), new IntegerMarshaller(), baseTableMock);
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
        when(baseTableMock.contains(STRING_ROW_KEY_1, STRING_COLUMN_KEY_1)).thenReturn(true);

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
        when(baseTableMock.containsRow(STRING_ROW_KEY_1)).thenReturn(true);

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
        when(baseTableMock.containsColumn(STRING_COLUMN_KEY_1)).thenReturn(true);

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
        when(baseTableMock.containsValue(STRING_VALUE_1)).thenReturn(true);

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
        when(baseTableMock.get(STRING_ROW_KEY_1, STRING_COLUMN_KEY_1)).thenReturn(STRING_VALUE_1);

        assertThat(transformingTable.get(ROW_KEY_1, COLUMN_KEY_1), is(equalTo(VALUE_1)));
    }

    // isEmpty

    public void isEmpty_delegates() {
        when(baseTableMock.isEmpty()).thenReturn(true);

        assertThat(transformingTable.isEmpty(), is(equalTo(true)));
    }

    // size

    public void size_delegates() {
        when(baseTableMock.size()).thenReturn(10);

        assertThat(transformingTable.size(), is(equalTo(10)));
    }

    // clear

    public void clear_delegates() {
        transformingTable.clear();

        verify(baseTableMock).clear();
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
        when(baseTableMock.put(STRING_ROW_KEY_1, STRING_COLUMN_KEY_1, STRING_VALUE_1)).thenReturn(STRING_VALUE_1);

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
        when(baseTableMock.remove(STRING_ROW_KEY_1, STRING_COLUMN_KEY_1)).thenReturn(STRING_VALUE_1);

        assertThat(transformingTable.remove(ROW_KEY_1, COLUMN_KEY_1), is(equalTo(VALUE_1)));
    }

    // cell set
    public void cellSet_delegates_to_backing_table() {
        Set<Table.Cell<String, String, String>> cellSet = Collections.singleton(Tables.immutableCell(STRING_ROW_KEY_1, STRING_COLUMN_KEY_1, STRING_VALUE_1));
        when(baseTableMock.cellSet()).thenReturn(cellSet);

        Table.Cell<Float, Long, Integer> expectedCell = Tables.immutableCell(ROW_KEY_1, COLUMN_KEY_1, VALUE_1);

        //noinspection unchecked
        assertThat(transformingTable.cellSet(), containsInAnyOrder(expectedCell));
    }

    // column key set

    @Test
    public void columnKeySet_delegates_to_backing_table() throws UnsupportedEncodingException {
        when(baseTableMock.columnKeySet()).thenReturn(ImmutableSet.of(STRING_COLUMN_KEY_1, STRING_COLUMN_KEY_2));

        assertThat(transformingTable.columnKeySet(), containsInAnyOrder(COLUMN_KEY_1, COLUMN_KEY_2));
    }

    // row key set

    @Test
    public void rowKeySet_delegates_to_backing_table() throws UnsupportedEncodingException {
        when(baseTableMock.rowKeySet()).thenReturn(ImmutableSet.of(STRING_ROW_KEY_1, STRING_ROW_KEY_2));

        assertThat(transformingTable.rowKeySet(), containsInAnyOrder(ROW_KEY_1, ROW_KEY_2));
    }

    // values

    @Test
    public void value_delegates_to_backing_table() throws UnsupportedEncodingException {
        when(baseTableMock.values()).thenReturn(ImmutableSet.of(STRING_VALUE_1, STRING_VALUE_2));

        assertThat(transformingTable.values(), containsInAnyOrder(VALUE_1, VALUE_2));
    }

    // put all delegates
    @Test
    public void put_all_delegates() {
        Table<Float, Long, Integer> tableToPut = new ImmutableTable.Builder().
                put(ROW_KEY_1, COLUMN_KEY_1, VALUE_1).
                put(ROW_KEY_2, COLUMN_KEY_2, VALUE_2).build();

        transformingTable.putAll(tableToPut);

        verify(baseTableMock).put(STRING_ROW_KEY_1, STRING_COLUMN_KEY_1, STRING_VALUE_1);
        verify(baseTableMock).put(STRING_ROW_KEY_2, STRING_COLUMN_KEY_2, STRING_VALUE_2);
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
