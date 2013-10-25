package com.yammer.collections.guava.azure;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RowMapViewTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Table<Integer, Long, String> backingTableMock;
    @Mock
    private Map<Long, String> columnMock;
    @Mock
    private Map<Long, String> columnMock2;
    private RowMapView<Integer, Long, String> rowMapView;

    @Before
    public void setUp() {
        rowMapView = new RowMapView<>(backingTableMock);
    }

    @Test
    public void size_delegates() {
        when(backingTableMock.rowKeySet().size()).thenReturn(3);

        assertThat(rowMapView.size(), is(equalTo(3)));
    }

    @Test
    public void isEmpty_delegates() {
        when(backingTableMock.isEmpty()).thenReturn(true);

        assertThat(rowMapView.isEmpty(), is(equalTo(true)));
    }

    @Test
    public void containsKey_delegates_to_contains_row() {
        when(backingTableMock.containsRow(2)).thenReturn(true);

        assertThat(rowMapView.containsKey(2), is(equalTo(true)));
    }

    @Test
    public void when_contains_value_on_non_entry_than_false() {
        assertThat(rowMapView.containsValue(new Object()), is(equalTo(false)));
    }

    @Test
    public void when_contains_value_on_entry_with_badly_typed_key_than_false() {
        assertThat(rowMapView.containsValue(new TestEntry<>("ala", "ma")), is(equalTo(false)));
    }

    @Test
    public void when_contain_on_correctly_typed_entry_then_delegates_to_backig_table() {
        Map.Entry<Long, String> entry = new TestEntry<>(1L, "ala");
        when(backingTableMock.column(entry.getKey()).containsValue(entry.getValue())).thenReturn(true);

        assertThat(rowMapView.containsValue(entry), is(equalTo(true)));
    }

    @Test
    public void when_get_on_key_of_correct_type_and_delegate_returned_map_is_empty_then_null_returned() {
        when(backingTableMock.row(2).isEmpty()).thenReturn(true);

        assertThat(rowMapView.get(2), is(nullValue()));
    }

    @Test
    public void when_get_on_key_of_correct_type_then_delegates() {
        when(backingTableMock.row(2)).thenReturn(columnMock);

        assertThat(rowMapView.get(2), is(equalTo(columnMock)));
    }

    @Test
    public void put_removes_old_values() {
        when(backingTableMock.row(1)).thenReturn(columnMock);

        rowMapView.put(1, new HashMap<Long, String>());

        verify(columnMock).clear();
    }

    @Test
    public void put_delegates_on_non_empty_map() {
        Map<Long, String> valueToPut = ImmutableMap.of(1L, "ala");
        when(backingTableMock.row(1)).thenReturn(columnMock);

        rowMapView.put(1, valueToPut);

        verify(columnMock).putAll(valueToPut);
    }

    @Test
    public void put_doesn_not_delegate_on_empty_map() {
        Map<Long, String> valueToPut = ImmutableMap.of();
        when(backingTableMock.row(1)).thenReturn(columnMock);

        rowMapView.put(1, valueToPut);

        verify(columnMock, never()).putAll(valueToPut);
    }

    @Test
    public void remove_removes_all_values_for_row() {
        when(backingTableMock.row(1)).thenReturn(columnMock);

        rowMapView.remove(1);

        verify(columnMock).clear();
    }

    @Test
    public void putAll_puts_every_map() {
        Map<Long, String> map1 = ImmutableMap.of(3L, "ala");
        Map<Long, String> map2 = ImmutableMap.of(4L, "ma");
        when(backingTableMock.row(1)).thenReturn(columnMock);
        when(backingTableMock.row(2)).thenReturn(columnMock2);

        Map<Integer, Map<Long, String>> mapToPut = ImmutableMap.of(
                1, map1,
                2, map2
        );

        rowMapView.putAll(mapToPut);

        verify(columnMock).putAll(map1);
        verify(columnMock2).putAll(map2);
    }

    @Test
    public void clear_delegates() {
        rowMapView.clear();

        verify(backingTableMock).clear();
    }

    @Test
    public void keySet_delegates() {
        Set<Integer> keySet = ImmutableSet.of(1, 2);
        when(backingTableMock.rowKeySet()).thenReturn(keySet);

        assertThat(rowMapView.keySet(), is(equalTo(keySet)));
    }

    @Test
    public void values_returns_correct_values() {
        when(backingTableMock.row(1)).thenReturn(columnMock);
        when(backingTableMock.row(2)).thenReturn(columnMock2);
        when(backingTableMock.rowKeySet()).thenReturn(ImmutableSet.of(1, 2));

        assertThat(rowMapView.values(), containsInAnyOrder(columnMock, columnMock2));
    }

    @Test
    public void entrySet_returns_correct_values() {
        when(backingTableMock.row(1)).thenReturn(columnMock);
        when(backingTableMock.row(2)).thenReturn(columnMock2);
        when(backingTableMock.rowKeySet()).thenReturn(ImmutableSet.of(1, 2));

        Map.Entry<Integer, Map<Long, String>> expectedEntry1 = new TestEntry<>(1, backingTableMock.row(1));
        Map.Entry<Integer, Map<Long, String>> expectedEntry2 = new TestEntry<>(2, backingTableMock.row(2));

        Iterator<Map.Entry<Integer, Map<Long, String>>> entrySetIterator = rowMapView.entrySet().iterator();


        Map.Entry<Integer, Map<Long, String>> foundEntry1 = entrySetIterator.next();
        Map.Entry<Integer, Map<Long, String>> foundEntry2 = entrySetIterator.next();
        assertThat(entrySetIterator.hasNext(), is(equalTo(false)));

        if (expectedEntry1.equals(foundEntry1)) {
            assertThat(expectedEntry2.equals(foundEntry2), is(equalTo(true)));
        } else if (expectedEntry1.equals(foundEntry2)) {
            assertThat(expectedEntry2.equals(foundEntry1), is(equalTo(true)));
        } else {
            fail();
        }
    }


    // ------------------------------
    // Utilities
    // ------------------------------

    private static final class TestEntry<K, V> implements Map.Entry<K, V> {
        private final K key;
        private final V value;

        private TestEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || !(o instanceof Map.Entry)) return false;

            Map.Entry testEntry = (Map.Entry) o;

            if (key != null ? !key.equals(testEntry.getKey()) : testEntry.getKey() != null) return false;
            if (value != null ? !value.equals(testEntry.getValue()) : testEntry.getValue() != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = key != null ? key.hashCode() : 0;
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }
    }


}
