package com.yammer.collections.guava.azure;

import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StringEtityIterableSetTest {
    // TODO change API to call for iterable rather than have it passed in (it will fix inconsistency issues)
    @Mock
    private StringEntity stringEntity1Mock;
    @Mock
    private StringEntity stringEntity2Mock;
    @Mock
    private StringAzureTable azureTableMock;

    private Iterable<StringEntity> stringEntityIterable;

    private StringEtityIterableSet set;

    @Before
    public void setUp() {
        stringEntityIterable = Arrays.asList(stringEntity1Mock, stringEntity2Mock);
        set = new StringEtityIterableSet(azureTableMock, stringEntityIterable);
    }

    @Test
    public void size_returns_correct_value() {
        assertThat(set.size(), is(equalTo(2)));
    }

    @Test
    public void is_returns_false_on_non_empty_set() {
        assertThat(set.isEmpty(), is(equalTo(false)));
    }

    @Test
    public void is_returns_true_on_empty_set() {
        set = new StringEtityIterableSet(azureTableMock, Collections.<StringEntity>emptyList());

        assertThat(set.isEmpty(), is(equalTo(true)));
    }

    @Test
    public void contains_on_non_table_cell_returns_false() {
        assertThat(set.contains(new Object()), is(equalTo(false)));
    }

    @Test
    public void contains_delegates_to_table() {
        Object o1 = new Object();
        Object o2 = new Object();
        Table.Cell<Object, Object, Object> cell = Tables.immutableCell(o1, o2, new Object());
        when(azureTableMock.contains(o1, o2)).thenReturn(true);

        assertThat(set.contains(cell), is(equalTo(true)));
    }




}
