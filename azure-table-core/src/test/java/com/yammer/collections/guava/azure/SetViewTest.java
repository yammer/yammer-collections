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
public class SetViewTest {
    private static final Long L1 = 1L;
    private static final Long L2 = 2L;
    private static final Long L1_DUPLICATE = 1L;
    private static final Integer SIZE = 2;
    @Mock
    private AbstractCollectionView<Long> abstractCollectionViewMock;
    private SetView<Long> setView;

    @Before
    public void setUp() {
        setView = SetView.fromSetCollectionView(abstractCollectionViewMock);
    }

    @Test
    public void size_delegates() {
        when(abstractCollectionViewMock.size()).thenReturn(SIZE);

        assertThat(setView.size(), is(equalTo(SIZE)));
    }

    @Test
    public void isEmpty_delegates() {
        when(abstractCollectionViewMock.isEmpty()).thenReturn(true);

        assertThat(setView.isEmpty(), is(equalTo(true)));
    }

    @Test
    public void contains_delegates() {
        when(abstractCollectionViewMock.contains(L1)).thenReturn(true);

        assertThat(setView.contains(L1), is(equalTo(true)));
    }

    @Test
    public void iterator_delegates() {
        when(abstractCollectionViewMock.iterator()).thenReturn(Arrays.asList(L1, L2).iterator());

        assertThat(setView, containsInAnyOrder(L1, L2));
    }

    @Test
    public void if_underlying_collection_is_a_multiset_then_this_collection_does_not_guarantee_contract() {
        when(abstractCollectionViewMock.iterator()).thenReturn(Arrays.asList(L1, L2, L1_DUPLICATE).iterator());

        assertThat(setView, containsInAnyOrder(L1, L2, L1_DUPLICATE));
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
        when(abstractCollectionViewMock.containsAll(Arrays.asList(L1, L2))).thenReturn(true);

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
