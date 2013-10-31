package com.yammer.collections.guava.azure;


import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;

import static com.yammer.collections.guava.azure.AzureEntityUtil.decode;
import static com.yammer.collections.guava.azure.AzureTestUtil.ENCODE_CELL;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@SuppressWarnings({"InstanceVariableMayNotBeInitialized", "SuspiciousMethodCalls"})
@RunWith(MockitoJUnitRunner.class)
public class AbstractCollectionViewTest {
    private static final String ROW_KEY_1 = "rown_name_1";
    private static final String ROW_KEY_2 = "row_name_2";
    private static final String COLUMN_KEY_1 = "column_key_1";
    private static final String COLUMN_KEY_2 = "column_key_2";
    private static final String VALUE_1 = "value1";
    private static final String VALUE_2 = "value3";
    private static final Long LONG_VALUE_1 = 1L;
    private static final Long LONG_VALUE_2 = 2L;
    private static final Table.Cell<String, String, String> CELL_1 = Tables.immutableCell(ROW_KEY_1, COLUMN_KEY_1, VALUE_1);
    private static final Table.Cell<String, String, String> CELL_2 = Tables.immutableCell(ROW_KEY_2, COLUMN_KEY_2, VALUE_2);
    private static final Function<AzureEntity, Long> LONG_EXTRACTOR = new Function<AzureEntity, Long>() {
        @Override
        public Long apply(AzureEntity input) {
            String decoded = decode(input.getValue());
            if (decoded.equals(VALUE_1)) {
                return LONG_VALUE_1;
            }

            return LONG_VALUE_2;
        }
    };
    @Mock
    private Iterable<AzureEntity> stringEntityIterableMock;
    private AbstractCollectionView<Long> abstractCollectionView;

    @Before
    public void setUp() {
        abstractCollectionView = new AbstractCollectionView<Long>(
                LONG_EXTRACTOR) {
            @Override
            protected Iterable<AzureEntity> getBackingIterable() {
                return stringEntityIterableMock;
            }
        };
    }

    // rename test names
    @Test
    public void size_returns_correct_value() throws StorageException {
        setAzureTableToContain(CELL_1, CELL_2);

        assertThat(abstractCollectionView.size(), is(equalTo(2)));
    }

    @Test
    public void is_returns_false_on_non_empty_collection() throws StorageException {
        setAzureTableToContain(CELL_1);

        assertThat(abstractCollectionView.isEmpty(), is(equalTo(false)));
    }

    @Test
    public void is_returns_true_on_empty_collection() throws StorageException {
        setAzureTableToContain();

        assertThat(abstractCollectionView.isEmpty(), is(equalTo(true)));
    }

    @Test
    public void contains_on_wrong_type_returns_false() throws StorageException {
        setAzureTableToContain(CELL_1);

        assertThat(abstractCollectionView.contains(new Object()), is(equalTo(false)));
    }

    @Test
    public void contain_returns_true_when_object_exists_in_collection() throws StorageException {
        setAzureTableToContain(CELL_1);

        assertThat(abstractCollectionView.contains(LONG_VALUE_1), is(equalTo(true)));
    }

    @Test
    public void contain_returns_false_when_object_does_not_exist_in_collection() throws
            StorageException {
        setAzureTableToContain();

        assertThat(abstractCollectionView.contains(LONG_VALUE_1), is(equalTo(false)));
    }

    @Test
    public void iterator_contains_contained_entities() throws StorageException {
        setAzureTableToContain(CELL_1, CELL_2);

        assertThat(abstractCollectionView, containsInAnyOrder(LONG_VALUE_1, LONG_VALUE_2));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void add_not_supported() {
        abstractCollectionView.add(LONG_VALUE_1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void remove_not_supported() {
        abstractCollectionView.remove(LONG_VALUE_2);
    }

    @Test
    public void when_contains_all_then_contains_all_returns_true() throws StorageException {
        setAzureTableToContain(CELL_1, CELL_2);

        assertThat(abstractCollectionView.containsAll(Arrays.asList(LONG_VALUE_1, LONG_VALUE_2)), is(equalTo(true)));
    }

    @Test
    public void when_does_not_contain_all_then_returns_false() throws StorageException {
        setAzureTableToContain(CELL_2);

        assertThat(abstractCollectionView.containsAll(Arrays.asList(LONG_VALUE_1, LONG_VALUE_2)), is(equalTo(false)));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void add_all_not_supported() {
        abstractCollectionView.addAll(Arrays.asList(LONG_VALUE_1, LONG_VALUE_2));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void remove_all_not_supported() {
        abstractCollectionView.removeAll(Arrays.asList(LONG_VALUE_1, LONG_VALUE_2));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void clear_all_unsupported() throws StorageException {
        abstractCollectionView.clear();
    }

    //----------------------
    // Utilities
    //----------------------

    @SafeVarargs
    private final void setAzureTableToContain(Table.Cell<String, String, String>... cells) {
        when(stringEntityIterableMock.iterator()).thenReturn(
                Iterables.transform(Arrays.asList(cells), ENCODE_CELL).iterator()
        );
    }

}
