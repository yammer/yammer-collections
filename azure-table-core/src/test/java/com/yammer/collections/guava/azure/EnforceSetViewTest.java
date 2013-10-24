package com.yammer.collections.guava.azure;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EnforceSetViewTest {
    private static final Long L1 = 1L;
    private static final Long L2 = 2L;
    private static final Integer SIZE = 2;
    @Mock
    private CollectionView<Long> collectionViewMock;
    private SetView<Long> setView;

    @Before
    public void setUp() {
        setView = SetView.fromCollectionView(collectionViewMock);
    }

    @Test
    public void size_computed_correctly() {
        when(collectionViewMock.iterator()).thenReturn(Arrays.asList(L1, L2, L2, L1, L1, L2, L1, L2, L1, L1, L1).iterator());

        assertThat(setView.size(), is(equalTo(SIZE)));
    }

    @Test
    public void isEmpty_delegates() {
        when(collectionViewMock.isEmpty()).thenReturn(true);

        assertThat(setView.isEmpty(), is(equalTo(true)));
    }

    @Test
    public void contains_delegates() {
        when(collectionViewMock.contains(L1)).thenReturn(true);

        assertThat(setView.contains(L1), is(equalTo(true)));
    }

    @Test
    public void iterator_delegates() {
        when(collectionViewMock.iterator()).thenReturn(Arrays.asList(L1, L2).iterator());

        assertThat(setView, containsInAnyOrder(L1, L2));
    }

    @Test
    public void if_underlying_collection_is_a_multiset_then_this_collection_is_a_set() {
        when(collectionViewMock.iterator()).thenReturn(Arrays.asList(L1, L2, L2, L1, L1, L2, L1, L2, L1, L1, L1).iterator());

        assertThat(setView, containsInAnyOrder(L1, L2));
    }



    @Test(expected = UnsupportedOperationException.class)
    public void add_not_supported() {
        setView.add(L1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void remove_not_supported() {
        setView.remove(L2);
    }

    @Test
    public void when_contains_all_delegates() {
        when(collectionViewMock.containsAll(Arrays.asList(L1, L2))).thenReturn(true);

        assertThat(setView.containsAll(Arrays.asList(L1, L2)), is(equalTo(true)));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void remove_all_unsupported() {
        setView.removeAll(Arrays.asList(L1, L2));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void clear_unsupported() {
        setView.clear();
    }

}
