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
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Set;

import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@SuppressWarnings("InstanceVariableMayNotBeInitialized")
@RunWith(MockitoJUnitRunner.class)
public class TransformingSetTest {
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
    private Set<String> backingSetMock;
    private Set<Integer> transformingSet;

    @Before
    public void setUp() {
        transformingSet = TransformingSet.create(backingSetMock, TO_FUNCTION, FROM_FUNCTION);
    }

    @Test
    public void equals_returns_false_on_nonequal_collection() {
        when(backingSetMock.iterator()).thenReturn(asList(T_VALUE_1, T_VALUE_2).iterator());

        assertThat(transformingSet.equals(ImmutableSet.of(F_VALUE_1, F_VALUE_OTHER)), is(equalTo(false)));
    }

    @Test
    public void equals_returns_false_on_shorter_collection() {
        when(backingSetMock.iterator()).thenReturn(asList(T_VALUE_1, T_VALUE_2).iterator());

        assertThat(transformingSet.equals(ImmutableSet.of(F_VALUE_1)), is(equalTo(false)));
    }

    @Test
    public void equals_returns_false_on_longer_collection() {
        when(backingSetMock.iterator()).thenReturn(asList(T_VALUE_1, T_VALUE_2).iterator());

        assertThat(transformingSet.equals(ImmutableSet.of(F_VALUE_1, F_VALUE_2, F_VALUE_OTHER)), is(equalTo(false)));
    }

    @Test
    public void equals_returns_false_on_different_collection_type() {
        when(backingSetMock.iterator()).thenReturn(asList(T_VALUE_1, T_VALUE_2).iterator());

        assertThat(transformingSet.equals(ImmutableList.of(F_VALUE_1, F_VALUE_2)), is(equalTo(false)));
    }


}
