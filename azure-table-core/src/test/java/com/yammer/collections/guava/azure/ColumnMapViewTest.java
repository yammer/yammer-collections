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
public class ColumnMapViewTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Table<Integer, Long, String> backingTableMock;
    @Mock
    private Map<Integer, String> rowMock;
    @Mock
    private Map<Integer, String> rowMock2;
    private ColumnMapView<Integer, Long, String> columnMapView;

    @Before
    public void setUp() {
        columnMapView = new ColumnMapView<>(backingTableMock);
    }

    @Test
    public void size_delegates() {
        when(backingTableMock.columnKeySet().size()).thenReturn(3);

        assertThat(columnMapView.size(), is(equalTo(3)));
    }

    @Test
    public void isEmpty_delegates() {
        when(backingTableMock.isEmpty()).thenReturn(true);

        assertThat(columnMapView.isEmpty(), is(equalTo(true)));
    }

    @Test
    public void containsKey_delegates_to_contains_row() {
        when(backingTableMock.containsColumn(2)).thenReturn(true);

        assertThat(columnMapView.containsKey(2), is(equalTo(true)));
    }

    @Test
    public void when_contains_value_on_non_entry_than_false() {
        assertThat(columnMapView.containsValue(new Object()), is(equalTo(false)));
    }

    @Test
    public void when_contains_value_on_entry_with_badly_typed_key_than_false() {
        assertThat(columnMapView.containsValue(new TestEntry<>("ala", "ma")), is(equalTo(false)));
    }

    @Test
    public void when_contain_on_correctly_typed_entry_then_delegates_to_backig_table() {
        Map.Entry<Integer, String> entry = new TestEntry<>(1, "ala");
        when(backingTableMock.row(entry.getKey()).containsValue(entry.getValue())).thenReturn(true);

        assertThat(columnMapView.containsValue(entry), is(equalTo(true)));
    }

    @Test
    public void when_get_on_key_of_correct_type_and_delegate_returned_map_is_empty_then_null_returned() {
        when(backingTableMock.column(2L).isEmpty()).thenReturn(true);

        assertThat(columnMapView.get(2L), is(nullValue()));
    }

    @Test
    public void when_get_on_key_of_correct_type_then_delegates() {
        when(backingTableMock.column(2L)).thenReturn(rowMock);

        assertThat(columnMapView.get(2L), is(equalTo(rowMock)));
    }

    @Test
    public void put_removes_old_values() {
        when(backingTableMock.column(1L)).thenReturn(rowMock);

        columnMapView.put(1L, new HashMap<Integer, String>());

        verify(rowMock).clear();
    }

    @Test
    public void put_delegates_on_non_empty_map() {
        Map<Integer, String> valueToPut = ImmutableMap.of(1, "ala");
        when(backingTableMock.column(1L)).thenReturn(rowMock);

        columnMapView.put(1L, valueToPut);

        verify(rowMock).putAll(valueToPut);
    }

    @Test
    public void put_doesn_not_delegate_on_empty_map() {
        Map<Integer, String> valueToPut = ImmutableMap.of();
        when(backingTableMock.column(1L)).thenReturn(rowMock);

        columnMapView.put(1L, valueToPut);

        verify(rowMock, never()).putAll(valueToPut);
    }

    @Test
    public void remove_removes_all_values_for_row() {
        when(backingTableMock.column(1L)).thenReturn(rowMock);

        columnMapView.remove(1L);

        verify(rowMock).clear();
    }

    @Test
    public void putAll_puts_every_map() {
        Map<Integer, String> map1 = ImmutableMap.of(3, "ala");
        Map<Integer, String> map2 = ImmutableMap.of(4, "ma");
        when(backingTableMock.column(1L)).thenReturn(rowMock);
        when(backingTableMock.column(2L)).thenReturn(rowMock2);

        Map<Long, Map<Integer, String>> mapToPut = ImmutableMap.of(
                1L, map1,
                2L, map2
        );

        columnMapView.putAll(mapToPut);

        verify(rowMock).putAll(map1);
        verify(rowMock2).putAll(map2);
    }

    @Test
    public void clear_delegates() {
        columnMapView.clear();

        verify(backingTableMock).clear();
    }

    @Test
    public void keySet_delegates() {
        Set<Long> keySet = ImmutableSet.of(1L, 2L);
        when(backingTableMock.columnKeySet()).thenReturn(keySet);

        assertThat(columnMapView.keySet(), is(equalTo(keySet)));
    }

    @Test
    public void values_returns_correct_values() {
        when(backingTableMock.column(1L)).thenReturn(rowMock);
        when(backingTableMock.column(2L)).thenReturn(rowMock2);
        when(backingTableMock.columnKeySet()).thenReturn(ImmutableSet.of(1L, 2L));

        assertThat(columnMapView.values(), containsInAnyOrder(rowMock, rowMock2));
    }

    @Test
    public void entrySet_returns_correct_values() {
        when(backingTableMock.column(1L)).thenReturn(rowMock);
        when(backingTableMock.column(2L)).thenReturn(rowMock2);
        when(backingTableMock.columnKeySet()).thenReturn(ImmutableSet.of(1L, 2L));

        Map.Entry<Long, Map<Integer, String>> expectedEntry1 = new TestEntry<>(1L, backingTableMock.column(1L));
        Map.Entry<Long, Map<Integer, String>> expectedEntry2 = new TestEntry<>(2L, backingTableMock.column(2L));

        Iterator<Map.Entry<Long, Map<Integer, String>>> entrySetIterator = columnMapView.entrySet().iterator();

        Map.Entry<Long, Map<Integer, String>> foundEntry1 = entrySetIterator.next();
        Map.Entry<Long, Map<Integer, String>> foundEntry2 = entrySetIterator.next();
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
