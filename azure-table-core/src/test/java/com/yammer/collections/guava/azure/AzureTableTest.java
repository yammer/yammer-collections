package com.yammer.collections.guava.azure;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.yammer.collections.guava.azure.AzureTable;
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
public class AzureTableTest {
    private static final Float ROW_KEY = 0.5f;
    private static final Long COLUMN_KEY = 23l;
    private static final Long COLUMN_KEY_2 = 2343l;
    private static final Integer VALUE = 1;
    private static final String STRING_ROW_KEY = ROW_KEY.toString();
    private static final String STRING_COLUMN_KEY = COLUMN_KEY.toString();
    private static final String STRING_VALUE = VALUE.toString();
    private static final String STRING_COLUMN_KEY_2 = COLUMN_KEY_2.toString();
    private AzureTable<Float, Long, Integer> azureTable;
    @Mock
    private Table<String, String, String> stringTableMock;

    @Before
    public void setUp() {
        azureTable = new AzureTable(new FloatMarshaller(), new LongMarshaller(), new IntegerMarshaller(), stringTableMock);
    }

    // contains

    @Test(expected = NullPointerException.class)
    public void when_contains_and_row_key_null_thenError() {
        azureTable.contains(null, COLUMN_KEY);
    }

    @Test(expected = NullPointerException.class)
    public void when_contains_and_column_key_null_thenError() {
        azureTable.contains(ROW_KEY, null);
    }

    @Test
    public void when_contains_and_row_key_of_wrong_class_then_false() {
        assertThat(azureTable.contains(new Object(), COLUMN_KEY), is(equalTo(false)));
    }

    @Test
    public void when_contains_and_column_key_of_wrong_class_then_false() {
        assertThat(azureTable.contains(ROW_KEY, new Object()), is(equalTo(false)));
    }

    @Test
    public void contains_delegates_to_backing_table() {
        when(stringTableMock.contains(STRING_ROW_KEY, STRING_COLUMN_KEY)).thenReturn(true);

        assertThat(azureTable.contains(ROW_KEY, COLUMN_KEY), is(equalTo(true)));
    }

    // containsRow

    @Test(expected = NullPointerException.class)
    public void when_containsRow_and_row_key_null_thenError() {
        azureTable.containsRow(null);
    }

    @Test
    public void when_containsRow_and_row_key_of_wrong_class_then_false() {
        assertThat(azureTable.containsRow(new Object()), is(equalTo(false)));
    }

    @Test
    public void containsRow_delegates_to_backing_table() {
        when(stringTableMock.containsRow(STRING_ROW_KEY)).thenReturn(true);

        assertThat(azureTable.containsRow(ROW_KEY), is(equalTo(true)));
    }

    // contains column

    @Test(expected = NullPointerException.class)
    public void when_containsColumn_and_column_key_null_thenError() {
        azureTable.containsRow(null);
    }

    @Test
    public void when_containsColumn_and_column_key_of_wrong_class_then_false() {
        assertThat(azureTable.containsRow(new Object()), is(equalTo(false)));
    }

    @Test
    public void containsColumn_delegates_to_backing_table() {
        when(stringTableMock.containsColumn(STRING_COLUMN_KEY)).thenReturn(true);

        assertThat(azureTable.containsColumn(COLUMN_KEY), is(equalTo(true)));
    }


    // contains value

    @Test(expected = NullPointerException.class)
    public void when_containsValue_and_value_is_null_thenError() {
        azureTable.containsValue(null);
    }

    @Test
    public void when_containsValue_and_value_of_wrong_class_then_false() {
        assertThat(azureTable.containsValue(new Object()), is(equalTo(false)));
    }

    @Test
    public void containsValue_delegates_to_backing_table() {
        when(stringTableMock.containsValue(STRING_VALUE)).thenReturn(true);

        assertThat(azureTable.containsValue(VALUE), is(equalTo(true)));
    }

    // get

    @Test(expected = NullPointerException.class)
    public void when_get_and_row_key_null_thenError() {
        azureTable.get(null, COLUMN_KEY);
    }

    @Test(expected = NullPointerException.class)
    public void when_get_and_column_key_null_thenError() {
        azureTable.get(ROW_KEY, null);
    }

    @Test
    public void when_get_and_row_key_of_wrong_class_then_null() {
        assertThat(azureTable.get(new Object(), COLUMN_KEY), is(nullValue()));
    }

    @Test
    public void when_get_and_column_key_of_wrong_class_then_null() {
        assertThat(azureTable.get(ROW_KEY, new Object()), is(nullValue()));
    }

    @Test
    public void get_delegates_to_backing_table() {
        when(stringTableMock.get(STRING_ROW_KEY, STRING_COLUMN_KEY)).thenReturn(STRING_VALUE);

        assertThat(azureTable.get(ROW_KEY, COLUMN_KEY), is(equalTo(VALUE)));
    }

    // isEmpty

    public void isEmpty_delegates() {
        when(stringTableMock.isEmpty()).thenReturn(true);

        assertThat(azureTable.isEmpty(), is(equalTo(true)));
    }

    // size

    public void size_delegates() {
        when(stringTableMock.size()).thenReturn(10);

        assertThat(azureTable.size(), is(equalTo(10)));
    }

    // clear

    public void clear_delegates() {
        azureTable.clear();

        verify(stringTableMock).clear();
    }

    // put

    @Test(expected = NullPointerException.class)
    public void when_put_and_row_key_null_thenError() {
        azureTable.put(null, COLUMN_KEY, VALUE);
    }

    @Test(expected = NullPointerException.class)
    public void when_put_and_column_key_null_thenError() {
        azureTable.put(ROW_KEY, null, VALUE);
    }

    @Test(expected = NullPointerException.class)
    public void when_put_and_value_null_thenError() {
        azureTable.put(ROW_KEY, COLUMN_KEY, null);
    }

    @Test
    public void put_delegates_to_backing_table() {
        when(stringTableMock.put(STRING_ROW_KEY, STRING_COLUMN_KEY, STRING_VALUE)).thenReturn(STRING_VALUE);

        assertThat(azureTable.put(ROW_KEY, COLUMN_KEY, VALUE), is(equalTo(VALUE)));
    }

    // remove

    @Test(expected = NullPointerException.class)
    public void when_remove_and_row_key_null_thenError() {
        azureTable.remove(null, COLUMN_KEY);
    }

    @Test(expected = NullPointerException.class)
    public void when_remove_and_column_key_null_thenError() {
        azureTable.remove(ROW_KEY, null);
    }

    @Test
    public void when_remove_and_row_key_of_wrong_class_then_null() {
        assertThat(azureTable.remove(new Object(), COLUMN_KEY), is(nullValue()));
    }

    @Test
    public void when_remove_and_column_key_of_wrong_class_then_null() {
        assertThat(azureTable.remove(ROW_KEY, new Object()), is(nullValue()));
    }

    @Test
    public void remove_delegates_to_backing_table() {
        when(stringTableMock.remove(STRING_ROW_KEY, STRING_COLUMN_KEY)).thenReturn(STRING_VALUE);

        assertThat(azureTable.remove(ROW_KEY, COLUMN_KEY), is(equalTo(VALUE)));
    }

    // cell set
    public void cellSet_delegates_to_backing_table() {
        Set<Table.Cell<String, String, String>> cellSet = Collections.singleton(Tables.immutableCell(STRING_ROW_KEY, STRING_COLUMN_KEY, STRING_VALUE));
        when(stringTableMock.cellSet()).thenReturn(cellSet);

        Table.Cell<Float, Long, Integer> expectedCell = Tables.immutableCell(ROW_KEY, COLUMN_KEY, VALUE);

        assertThat(azureTable.cellSet(), containsInAnyOrder(expectedCell));
    }

    // column key set

    @Test
    public void columnKeySet_delegates_to_backing_table() throws UnsupportedEncodingException {
        when(stringTableMock.columnKeySet()).thenReturn(ImmutableSet.of(STRING_COLUMN_KEY, STRING_COLUMN_KEY_2));

        assertThat(azureTable.columnKeySet(), containsInAnyOrder(COLUMN_KEY, COLUMN_KEY_2));
    }


    // ----- stubs ----

    private class FloatMarshaller implements AzureTable.Marshaller<Float, String> {
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

    private class LongMarshaller implements AzureTable.Marshaller<Long, String> {

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

    private class IntegerMarshaller implements AzureTable.Marshaller<Integer, String> {
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
