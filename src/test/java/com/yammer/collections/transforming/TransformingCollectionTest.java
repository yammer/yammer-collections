/**
 * Copyright (c) Microsoft Corporation
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER
 * EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS
 * OF TITLE, FITNESS FOR A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing permissions and limitations under
 * the License.
 */
package com.yammer.collections.transforming;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@SuppressWarnings({"InstanceVariableMayNotBeInitialized", "SuspiciousMethodCalls"})
@RunWith(MockitoJUnitRunner.class)
public class TransformingCollectionTest {
    private static final Integer F_VALUE_1 = 11;
    private static final Integer F_VALUE_2 = 22;
    private static final Integer F_VALUE_OTHER = 33;
    private static final String T_VALUE_1 = F_VALUE_1.toString();
    private static final String T_VALUE_2 = F_VALUE_2.toString();
    private static final Function<Integer, String> TO_FUNCTION = new Function<Integer, String>() {
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
    private Collection<String> backingCollectionMock;
    @Captor
    private ArgumentCaptor<Collection<String>> collectionCaptor;
    private Collection<Integer> transformingCollection;

    @Before
    public void setUp() {
        transformingCollection = TransformingCollection.create(backingCollectionMock, TO_FUNCTION, FROM_FUNCTION);
    }

    @Test(expected = NullPointerException.class)
    public void backingCollection_cannot_be_null() {
        TransformingCollection.create(null, TO_FUNCTION, FROM_FUNCTION);
    }

    @Test(expected = NullPointerException.class)
    public void toFunction_cannot_be_null() {
        TransformingCollection.create(backingCollectionMock, null, FROM_FUNCTION);
    }

    @Test(expected = NullPointerException.class)
    public void fromFunction_cannot_be_null() {
        TransformingCollection.create(backingCollectionMock, TO_FUNCTION, null);
    }

    @Test
    public void size_delegates() {
        when(backingCollectionMock.size()).thenReturn(2);

        assertThat(transformingCollection.size(), is(equalTo(2)));
    }

    @Test
    public void isEmpty_delegates() {
        when(backingCollectionMock.isEmpty()).thenReturn(true);

        assertThat(transformingCollection.isEmpty(), is(equalTo(true)));
    }

    @Test
    public void contains_delegates() {
        when(backingCollectionMock.contains(T_VALUE_1)).thenReturn(true);

        assertThat(transformingCollection.contains(F_VALUE_1), is(equalTo(true)));
    }

    @Test
    public void containsAll_delegates() {
        when(backingCollectionMock.containsAll(collectionCaptor.capture())).thenReturn(true);

        assertThat(transformingCollection.containsAll(asList(F_VALUE_1, F_VALUE_2)), is(equalTo(true)));
        assertThat(collectionCaptor.getValue(), contains(T_VALUE_1, T_VALUE_2));
    }

    @Test
    public void contains_null_returns_false() {
        assertThat(transformingCollection.contains(null), is(equalTo(false)));
    }

    @Test
    public void contains_returns_false_if_object_of_wrong_type() {
        when(backingCollectionMock.contains(T_VALUE_1)).thenReturn(true);

        assertThat(transformingCollection.contains(11.0f), is(equalTo(false)));
    }

    @Test
    public void iterator_delegates() {
        when(backingCollectionMock.iterator()).thenReturn(asList(T_VALUE_1, T_VALUE_2).iterator());

        assertThat(transformingCollection, containsInAnyOrder(F_VALUE_1, F_VALUE_2));
    }

    @Test
    public void add_delegats() {
        when(backingCollectionMock.add(T_VALUE_1)).thenReturn(true);

        assertThat(transformingCollection.add(F_VALUE_1), is(equalTo(true)));
    }

    @Test
    public void addAll_delegats() {
        when(backingCollectionMock.addAll(collectionCaptor.capture())).thenReturn(true);

        assertThat(transformingCollection.addAll(asList(F_VALUE_1, F_VALUE_2)), is(equalTo(true)));
        assertThat(collectionCaptor.getValue(), contains(T_VALUE_1, T_VALUE_2));
    }

    @Test(expected = NullPointerException.class)
    public void add_null_not_allowed() {
        transformingCollection.add(null);
    }

    @Test
    public void remove_delegates() {
        when(backingCollectionMock.remove(T_VALUE_1)).thenReturn(true);

        assertThat(transformingCollection.remove(F_VALUE_1), is(equalTo(true)));
    }

    @Test
    public void removeAll_delegates() {
        when(backingCollectionMock.removeAll(collectionCaptor.capture())).thenReturn(true);

        assertThat(transformingCollection.removeAll(asList(F_VALUE_1, F_VALUE_2)), is(equalTo(true)));
        assertThat(collectionCaptor.getValue(), contains(T_VALUE_1, T_VALUE_2));
    }

    @Test
    public void remove_null_removes_false() {
        assertThat(transformingCollection.remove(null), is(equalTo(false)));
    }

    @Test
    public void remove_of_wrong_type_returns_false() {
        when(backingCollectionMock.remove(T_VALUE_1)).thenReturn(true);

        assertThat(transformingCollection.remove(11.0f), is(equalTo(false)));
    }

    @Test
    public void retainAll_delegates() {
        when(backingCollectionMock.retainAll(collectionCaptor.capture())).thenReturn(true);

        assertThat(transformingCollection.retainAll(asList(F_VALUE_1, F_VALUE_2)), is(equalTo(true)));
        assertThat(collectionCaptor.getValue(), contains(T_VALUE_1, T_VALUE_2));
    }

    @Test
    public void clear_delegates() {
        transformingCollection.clear();

        verify(backingCollectionMock).clear();
    }

    @Test
    public void equals_returns_true_on_equal_collection() {
        when(backingCollectionMock.iterator()).thenReturn(asList(T_VALUE_1, T_VALUE_2).iterator());

        assertThat(transformingCollection.equals(ImmutableSet.of(F_VALUE_1, F_VALUE_2)), is(equalTo(true)));
    }

    @Test
    public void equals_returns_false_on_nonequal_collection() {
        when(backingCollectionMock.iterator()).thenReturn(asList(T_VALUE_1, T_VALUE_2).iterator());

        assertThat(transformingCollection.equals(ImmutableSet.of(F_VALUE_1, F_VALUE_OTHER)), is(equalTo(false)));
    }

    @Test
    public void equals_returns_false_on_shorter_collection() {
        when(backingCollectionMock.iterator()).thenReturn(asList(T_VALUE_1, T_VALUE_2).iterator());

        assertThat(transformingCollection.equals(ImmutableSet.of(F_VALUE_1)), is(equalTo(false)));
    }

    @Test
    public void equals_returns_false_on_longer_collection() {
        when(backingCollectionMock.iterator()).thenReturn(asList(T_VALUE_1, T_VALUE_2).iterator());

        assertThat(transformingCollection.equals(ImmutableSet.of(F_VALUE_1, F_VALUE_2, F_VALUE_OTHER)), is(equalTo(false)));
    }


}
