package com.yammer.collections.guava.azure.transforming;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransformingMapTest {
    private static final Integer F_KEY_1 = 11;
    private static final Integer F_KEY_2 = 22;
    private static final Float F_VALUE_1 = 0.5f;
    private static final Float F_VALUE_2 = 0.8f;
    private static final Float F_VALUE_OTHER = 99.33f;
    private static final Map.Entry<Integer, Float> F_ENTRY_1 = new TestEntry<>(F_KEY_1, F_VALUE_1);
    private static final Map.Entry<Integer, Float> F_ENTRY_2 = new TestEntry<>(F_KEY_2, F_VALUE_2);
    private static final String T_KEY_1 = F_KEY_1.toString();
    private static final String T_KEY_2 = F_KEY_2.toString();
    private static final String T_VALUE_1 = F_VALUE_1.toString();
    private static final String T_VALUE_2 = F_VALUE_2.toString();
    private static final String T_VALUE_OTHER = F_VALUE_OTHER.toString();
    private static final Map.Entry<String, String> T_ENTRY_1 = new TestEntry<>(T_KEY_1, T_VALUE_1);
    private static final Map.Entry<String, String> T_ENTRY_2 = new TestEntry<>(T_KEY_2, T_VALUE_2);
    private static final Function<Integer, String> TO_KEY_FUNCTION = new Function<Integer, String>() {
        @Override
        public String apply(Integer input) {
            return input.toString();
        }
    };
    private static final Function<String, Integer> FROM_KEY_FUNCTION = new Function<String, Integer>() {
        @Override
        public Integer apply(String input) {
            return Integer.parseInt(input);
        }
    };
    private static final Function<Float, String> TO_VALUE_FUNCTION = new Function<Float, String>() {
        @Override
        public String apply(Float input) {
            return input.toString();
        }
    };
    private static final Function<String, Float> FROM_VALUE_FUNCTION = new Function<String, Float>() {
        @Override
        public Float apply(String input) {
            return Float.parseFloat(input);
        }
    };
    @Mock
    private Map<String, String> backingMapMock;
    @Mock
    private Map.Entry<String, String> backingEntryMock;
    private Map<Integer, Float> transfromingMap;

    @Before
    public void setUp() {
        transfromingMap = new TransformingMap<>(
                backingMapMock,
                TO_KEY_FUNCTION,
                FROM_KEY_FUNCTION,
                TO_VALUE_FUNCTION,
                FROM_VALUE_FUNCTION
        );
    }

    @Test
    public void size_delegates() {
        when(backingMapMock.size()).thenReturn(2);

        assertThat(transfromingMap.size(), is(equalTo(2)));
    }

    @Test
    public void isEmpty_delegates() {
        when(backingMapMock.isEmpty()).thenReturn(true);

        assertThat(transfromingMap.isEmpty(), is(equalTo(true)));
    }

    @Test
    public void containsValue_delegates() {
        when(backingMapMock.containsValue(T_VALUE_1)).thenReturn(true);

        assertThat(transfromingMap.containsValue(F_VALUE_1), is(equalTo(true)));
    }

    @Test
    public void containsValue_delegates_on_null_argument() {
        when(backingMapMock.containsValue(null)).thenReturn(true);

        assertThat(transfromingMap.containsValue(null), is(equalTo(true)));
    }

    @Test
    public void contains_value_returns_false_if_object_of_wrong_type() {
        when(backingMapMock.containsValue(T_VALUE_1)).thenReturn(true);

        assertThat(transfromingMap.containsValue("ala"), is(equalTo(false)));
    }

    @Test
    public void containsKey_delegates() {
        when(backingMapMock.containsKey(T_KEY_1)).thenReturn(true);

        assertThat(transfromingMap.containsKey(F_KEY_1), is(equalTo(true)));
    }

    @Test
    public void containsKey_returns_false_if_object_of_wrong_type() {
        when(backingMapMock.containsKey(T_KEY_1)).thenReturn(true);

        assertThat(transfromingMap.containsKey(11.0f), is(equalTo(false)));
    }

    @Test
    public void containsKey_delegates_on_null_argument() {
        when(backingMapMock.containsKey(null)).thenReturn(true);

        assertThat(transfromingMap.containsKey(null), is(equalTo(true)));
    }

    @Test
    public void keySet_delegates() {
        when(backingMapMock.keySet()).thenReturn(ImmutableSet.of(T_KEY_1, T_KEY_2));

        assertThat(transfromingMap.keySet(), containsInAnyOrder(F_KEY_1, F_KEY_2));
    }

    @Test
    public void values_delegates() {
        when(backingMapMock.values()).thenReturn(ImmutableList.of(T_VALUE_1, T_VALUE_2, T_VALUE_2));

        assertThat(transfromingMap.values(), containsInAnyOrder(F_VALUE_1, F_VALUE_2, F_VALUE_2));
    }

    @Test
    public void put_delegats() {
        when(backingMapMock.put(T_KEY_1, T_VALUE_1)).thenReturn(T_VALUE_OTHER);

        assertThat(transfromingMap.put(F_KEY_1, F_VALUE_1), is(equalTo(F_VALUE_OTHER)));
    }

    @Test
    public void put_on_null_key_delegates() {
        when(backingMapMock.put(null, T_VALUE_1)).thenReturn(T_VALUE_OTHER);

        assertThat(transfromingMap.put(null, F_VALUE_1), is(equalTo(F_VALUE_OTHER)));
    }

    @Test
    public void put_on_null_value_delegates() {
        when(backingMapMock.put(T_KEY_1, null)).thenReturn(T_VALUE_OTHER);

        assertThat(transfromingMap.put(F_KEY_1, null), is(equalTo(F_VALUE_OTHER)));
    }

    @Test
    public void put_returns_null_when_delegate_returns_null() {
        when(backingMapMock.put(T_KEY_1, T_VALUE_1)).thenReturn(null);

        assertThat(transfromingMap.put(F_KEY_1, F_VALUE_1), is(nullValue()));
    }

    @Test
    public void remove_delegates() {
        when(backingMapMock.remove(T_KEY_1)).thenReturn(T_VALUE_1);

        assertThat(transfromingMap.remove(F_KEY_1), is(equalTo(F_VALUE_1)));
    }

    @Test
    public void remove_on_null_key_delegates() {
        when(backingMapMock.remove(null)).thenReturn(T_VALUE_1);

        assertThat(transfromingMap.remove(null), is(equalTo(F_VALUE_1)));
    }

    @Test
    public void remove_returns_null_when_delegate_returns_null() {
        when(backingMapMock.remove(T_KEY_1)).thenReturn(null);

        assertThat(transfromingMap.remove(F_KEY_1), is(nullValue()));
    }

    @Test
    public void remove_of_wrong_type_returns_null() {
        when(backingMapMock.remove(T_KEY_1)).thenReturn(T_VALUE_1);

        assertThat(transfromingMap.remove(11.0f), is(nullValue()));
    }

    @Test
    public void get_delegates() {
        when(backingMapMock.get(T_KEY_1)).thenReturn(T_VALUE_1);

        assertThat(transfromingMap.get(F_KEY_1), is(equalTo(F_VALUE_1)));
    }

    @Test
    public void get_on_null_key_delegates() {
        when(backingMapMock.get(null)).thenReturn(T_VALUE_1);

        assertThat(transfromingMap.get(null), is(equalTo(F_VALUE_1)));
    }

    @Test
    public void get_returns_null_when_delegate_returns_null() {
        when(backingMapMock.remove(T_KEY_1)).thenReturn(null);

        assertThat(transfromingMap.get(F_KEY_1), is(nullValue()));
    }

    @Test
    public void get_of_wrong_type_returns_null() {
        when(backingMapMock.get(T_KEY_1)).thenReturn(T_VALUE_1);

        assertThat(transfromingMap.get(11.0f), is(nullValue()));
    }

    @Test
    public void clear_delegates() {
        transfromingMap.clear();

        verify(backingMapMock).clear();
    }

    @Test
    public void entrySet_delegates() {
        when(backingMapMock.entrySet()).thenReturn(ImmutableSet.of(T_ENTRY_1, T_ENTRY_2));

        Iterator<Map.Entry<Integer, Float>> entries = transfromingMap.entrySet().iterator();
        Map.Entry<Integer, Float> entry1 = entries.next();
        Map.Entry<Integer, Float> entry2 = entries.next();

        assertThat(entries.hasNext(), is(equalTo(false)));
        assertThat(
                (F_ENTRY_1.equals(entry1) && F_ENTRY_2.equals(entry2)) ||
                        (F_ENTRY_1.equals(entry2) && F_ENTRY_2.equals(entry1)),
                is(equalTo(true))
        );
    }

    @Test
    public void getKey_on_entry_delegates() {
        when(backingEntryMock.getKey()).thenReturn(T_KEY_1);
        when(backingMapMock.entrySet()).thenReturn(Collections.singleton(backingEntryMock));

        Map.Entry<Integer, Float> transformingEntry = Iterables.getFirst(transfromingMap.entrySet(), null);


        assertThat(transformingEntry.getKey(), is(equalTo(F_KEY_1)));
    }

    @Test
    public void getKey_on_entry_returns_null_when_delegateEntry_returns_null() {
        when(backingEntryMock.getKey()).thenReturn(null);
        when(backingMapMock.entrySet()).thenReturn(Collections.singleton(backingEntryMock));

        Map.Entry<Integer, Float> transformingEntry = Iterables.getFirst(transfromingMap.entrySet(), null);


        assertThat(transformingEntry.getKey(), is(nullValue()));
    }

    @Test
    public void getValue_on_entry_delegates() {
        when(backingEntryMock.getValue()).thenReturn(T_VALUE_1);
        when(backingMapMock.entrySet()).thenReturn(Collections.singleton(backingEntryMock));

        Map.Entry<Integer, Float> transformingEntry = Iterables.getFirst(transfromingMap.entrySet(), null);


        assertThat(transformingEntry.getValue(), is(equalTo(F_VALUE_1)));
    }

    @Test
    public void getValue_on_entry_returns_null_when_delegateEntry_returns_null() {
        when(backingEntryMock.getValue()).thenReturn(null);
        when(backingMapMock.entrySet()).thenReturn(Collections.singleton(backingEntryMock));

        Map.Entry<Integer, Float> transformingEntry = Iterables.getFirst(transfromingMap.entrySet(), null);


        assertThat(transformingEntry.getValue(), is(nullValue()));
    }

    @Test
    public void setValue_on_entry_delegates() {
        when(backingEntryMock.setValue(T_VALUE_2)).thenReturn(T_VALUE_1);
        when(backingMapMock.entrySet()).thenReturn(Collections.singleton(backingEntryMock));

        Map.Entry<Integer, Float> transformingEntry = Iterables.getFirst(transfromingMap.entrySet(), null);


        assertThat(transformingEntry.setValue(F_VALUE_2), is(equalTo(F_VALUE_1)));
    }

    @Test
    public void setValue_on_entry_returns_null_when_delegateEntry_returns_null() {
        when(backingEntryMock.setValue(T_VALUE_2)).thenReturn(null);
        when(backingMapMock.entrySet()).thenReturn(Collections.singleton(backingEntryMock));

        Map.Entry<Integer, Float> transformingEntry = Iterables.getFirst(transfromingMap.entrySet(), null);


        assertThat(transformingEntry.setValue(F_VALUE_2), is(nullValue()));
    }

    @Test
    public void setNullValue_on_entry_delegates() {
        when(backingEntryMock.setValue(null)).thenReturn(T_VALUE_1);
        when(backingMapMock.entrySet()).thenReturn(Collections.singleton(backingEntryMock));

        Map.Entry<Integer, Float> transformingEntry = Iterables.getFirst(transfromingMap.entrySet(), null);


        assertThat(transformingEntry.setValue(null), is(equalTo(F_VALUE_1)));
    }

    private final static class TestEntry<K, V> implements Map.Entry<K, V> {
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

            Map.Entry entry = (Map.Entry) o;

            if (key != null ? !key.equals(entry.getKey()) : entry.getKey() != null) return false;
            if (value != null ? !value.equals(entry.getValue()) : entry.getValue() != null) return false;

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
