package com.yammer.collections.guava.azure;

import com.google.common.base.Function;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Iterator;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransformingSetTest {
    private static final Integer F_VALUE_1 = 11;
    private static final Integer F_VAUE_2 = 22;
    private static final String T_VALUE_1 = F_VALUE_1.toString();
    private static final String T_VALUE_2 = F_VAUE_2.toString();
    private static final Function<Integer,String> TO_FUNCTION = new Function<Integer, String>() {
        @Override
        public String apply(Integer input) {
            return input.toString();
        }
    };
    private static final Function<String, Integer> FROM_FUNCTION = new Function<String, Integer>() {
        @Override
        public Integer apply(String input) {
            return Integer.parseInt(input);
        }
    };

    @Mock
    private Set<String> backingSetMock;

    private Set<Integer> transformingSet;

    @Before
    public void setUp() {
        transformingSet = new TransformingSet<>(backingSetMock, TO_FUNCTION, FROM_FUNCTION);
    }

    @Test
    public void size_delegates() {
        when(backingSetMock.size()).thenReturn(2);

        assertThat(transformingSet.size(), is(equalTo(2)));
    }

    @Test
    public void isEmpty_delegates() {
        when(backingSetMock.isEmpty()).thenReturn(true);

        assertThat(transformingSet.isEmpty(), is(equalTo(true)));
    }

    @Test
    public void contains_delegates() {
       when(backingSetMock.contains(T_VALUE_1)).thenReturn(true);

        assertThat(transformingSet.contains(F_VALUE_1), is(equalTo(true)));
    }

    @Test
    public void contains_returns_false_if_object_of_wrong_type() {
        when(backingSetMock.contains(T_VALUE_1)).thenReturn(true);

        assertThat(transformingSet.contains(11.0f), is(equalTo(false)));
    }

    @Test
    public void iterator_delegates() {
        when(backingSetMock.iterator()).thenReturn(asList(T_VALUE_1, T_VALUE_2).iterator());

        assertThat(transformingSet, containsInAnyOrder(F_VALUE_1, F_VAUE_2));
    }

    @Test
    public void add_delegats() {
        when(backingSetMock.add(T_VALUE_1)).thenReturn(true);

        assertThat(transformingSet.add(F_VALUE_1), is(equalTo(true)));
    }




}
