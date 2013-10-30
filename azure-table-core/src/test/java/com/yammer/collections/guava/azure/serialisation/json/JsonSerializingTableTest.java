package com.yammer.collections.guava.azure.serialisation.json;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.codehaus.jackson.annotate.JsonProperty;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class JsonSerializingTableTest {
    private static final Float ROW = 11.34f;
    private static final Long COLUMN = 123l;
    private static final TestValuePojo VALUE = new TestValuePojo("Michal", Arrays.asList(29, 1, 1980));
    private static final String SERIALIED_ROW = ROW.toString();
    private static final String SERIALIZED_COLUMN = COLUMN.toString();
    private static final String SERIALIZED_VALUE = "{\"name\":\"Michal\",\"numbers\":[29,1,1980]}";
    private Table<String, String, String> backingTable;
    private JsonSerializingTable<Float, Long, TestValuePojo> jsonSerializingTable;

    @Before
    public void setUp() {
        backingTable = HashBasedTable.create();
        jsonSerializingTable = new JsonSerializingTable<>(
                backingTable, Float.class, Long.class, TestValuePojo.class);
    }

    @Test
    public void put_correctly_serializes() {
        jsonSerializingTable.put(ROW, COLUMN, VALUE);

        assertThat(backingTable.get(SERIALIED_ROW, SERIALIZED_COLUMN), is(equalTo(SERIALIZED_VALUE)));
    }

    @Test
    public void get_correctly_deserializes() {
        backingTable.put(SERIALIED_ROW, SERIALIZED_COLUMN, SERIALIZED_VALUE);

        assertThat(jsonSerializingTable.get(ROW, COLUMN), is(equalTo(VALUE)));
    }

    public static class TestValuePojo {
        private final String name;
        private final Collection<Integer> numbers;


        public TestValuePojo(
                @JsonProperty("name") String name,
                @JsonProperty("numbers") Collection<Integer> numbers) {
            this.name = name;
            this.numbers = numbers;
        }

        public String getName() {
            return name;
        }

        public Collection<Integer> getNumbers() {
            return numbers;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestValuePojo that = (TestValuePojo) o;

            if (name != null ? !name.equals(that.name) : that.name != null) return false;
            if (numbers != null ? !numbers.equals(that.numbers) : that.numbers != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (numbers != null ? numbers.hashCode() : 0);
            return result;
        }
    }

}
