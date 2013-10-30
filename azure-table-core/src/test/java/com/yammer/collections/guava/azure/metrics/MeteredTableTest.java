package com.yammer.collections.guava.azure.metrics;


import com.google.common.collect.Table;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MeteredTableTest {

    @Mock
    private Table<Float, Long, Integer> backingTable;
    private Table<Float, Long, Integer> meteredTable;

    @Before
    public void setUp() {
        meteredTable = MeteredTable.create(backingTable);
    }

    @Ignore("write delegation tests") // TODO write delegation tests
    @Test
    public void test() {

    }

}
