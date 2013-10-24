package com.yammer.collections.guava.azure;


import com.google.common.base.Function;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.yammer.collections.guava.azure.StringEntityUtil.decode;


@RunWith(MockitoJUnitRunner.class)
public class PossibleSetCollectionViewTest {
    private static final String ROW_KEY_1 = "rown_name_1";
    private static final String ROW_KEY_2 = "row_name_2";
    private static final String COLUMN_KEY_1 = "column_key_1";
    private static final String COLUMN_KEY_2 = "column_key_2";
    private static final String VALUE_1 = "value1";
    private static final String VALUE_2 = "value3";
    private static final Long L1 = 1L;
    private static final Long L2 = 2L;
    private static final String TABLE_NAME = "secretie_table";
    private static final Table.Cell<String, String, String> CELL_1 = Tables.immutableCell(ROW_KEY_1, COLUMN_KEY_1, VALUE_1);
    private static final Table.Cell<String, String, String> CELL_2 = Tables.immutableCell(ROW_KEY_2, COLUMN_KEY_2, VALUE_2);
    private static final Function<StringEntity, Long> LONG_EXTRACTOR = new Function<StringEntity, Long>() {
        @Override
        public Long apply(StringEntity input) {
            String decoded = decode(input.getValue());
            if(decoded.equals(VALUE_1)) {
                return L1;
            }

            return L2;
        }
    };

    @Mock
    private StringAzureTable stringAzureTable;
    @Mock
    private StringTableCloudClient stringTableCloudClientMock;
    @Mock
    private StringTableRequestFactory stringTableRequestFactoryMock;

    private PossibleSetCollectionView<Long> possibleSetCollectionView;

    @Before
    public void setUp() {
        when(stringAzureTable.getTableName()).thenReturn(TABLE_NAME);
        possibleSetCollectionView = new PossibleSetCollectionView(
                stringAzureTable,
                LONG_EXTRACTOR,
                stringTableCloudClientMock,
                stringTableRequestFactoryMock);
    }
   // rename test names
    @Test
    public void size_returns_correct_value() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1, CELL_2);

        assertThat(possibleSetCollectionView.size(), is(equalTo(2)));
    }

    @Test
    public void is_returns_false_on_non_empty_collection() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1);

        assertThat(possibleSetCollectionView.isEmpty(), is(equalTo(false)));
    }

    @Test
    public void is_returns_true_on_empty_collection() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain();

        assertThat(possibleSetCollectionView.isEmpty(), is(equalTo(true)));
    }

    @Test
    public void contains_on_wrong_type_returns_false() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1);

        assertThat(possibleSetCollectionView.contains(new Object()), is(equalTo(false)));
    }

    @Test
    public void contain_returns_true_when_object_exists_in_collection() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1);

        assertThat(possibleSetCollectionView.contains(L1), is(equalTo(true)));
    }

    @Test
    public void contain_returns_false_when_object_does_not_exist_in_collection() throws UnsupportedEncodingException,
            StorageException {
        setAzureTableToContain();

        assertThat(possibleSetCollectionView.contains(L1), is(equalTo(false)));
    }

    @Test
    public void iterator_contains_contained_entities() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1, CELL_2);

        assertThat(possibleSetCollectionView, containsInAnyOrder(L1, L2));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void add_not_supported() {
        possibleSetCollectionView.add(L1);
    }


    @Test(expected = UnsupportedOperationException.class)
    public void remove_not_supported() {
        possibleSetCollectionView.remove(L2);

        verify(stringAzureTable).remove(ROW_KEY_1, COLUMN_KEY_1);
    }

    @Test
    public void when_contains_all_then_contains_all_returns_true() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_1, CELL_2);

        assertThat(possibleSetCollectionView.containsAll(Arrays.asList(L1, L2)), is(equalTo(true)));
    }

    @Test
    public void when_does_not_contain_all_then_returns_false() throws UnsupportedEncodingException, StorageException {
        setAzureTableToContain(CELL_2);

        assertThat(possibleSetCollectionView.containsAll(Arrays.asList(L1, L2)), is(equalTo(false)));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void add_all_not_supported() {
        possibleSetCollectionView.addAll(Arrays.asList(L1, L2));
    }


    @Test(expected = UnsupportedOperationException.class)
    public void remove_all_not_supported() {
        possibleSetCollectionView.removeAll(Arrays.asList(L1, L2));
    }


    @Test(expected = UnsupportedOperationException.class)
    public void clear_all_unsupported() throws UnsupportedEncodingException, StorageException {
        possibleSetCollectionView.clear();
    }

    //----------------------
    // Utilities
    //----------------------

    private void setAzureTableToContain(Table.Cell<String, String, String>... cells) throws UnsupportedEncodingException, StorageException {
        for(Table.Cell<String, String, String> cell : cells) {
            when(stringAzureTable.get(cell.getRowKey(), cell.getColumnKey())).thenReturn(cell.getValue());
            when(stringAzureTable.contains(cell.getRowKey(), cell.getColumnKey())).thenReturn(true);
            when(stringAzureTable.put(eq(cell.getRowKey()), eq(cell.getColumnKey()), anyString())).thenReturn(cell.getValue());
            when(stringAzureTable.remove(cell.getRowKey(), cell.getColumnKey())).thenReturn(cell.getValue());

        }
        AzureTestUtil.setAzureTableToContain(TABLE_NAME, stringTableRequestFactoryMock, stringTableCloudClientMock, cells);
    }


}
