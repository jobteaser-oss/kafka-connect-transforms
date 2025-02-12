package com.jobteaser.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class JoinFieldsTest {
    private final JoinFields<SourceRecord> smt = new JoinFields.Value<>();

    @AfterEach
    public void tearDown() throws Exception {
        smt.close();
    }

    @Test
    public void topLevelStructRequired() {
        assertThrows(DataException.class, () -> {
            smt.configure(Collections.emptyMap());
            smt.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
        });
    }

    @Test
    public void schemaDefaultConfigNoElement() {
        final Map<String, Object> props = new HashMap<>();
        smt.configure(props);

        final Schema initialStructSchema =
                SchemaBuilder.struct().name("name").version(1).doc("doc")
                        .field("abc", Schema.STRING_SCHEMA)
                        .build();
        final Struct initialStruct = new Struct(initialStructSchema)
                .put("abc", "def");


        final Schema expectedStructSchema =
                SchemaBuilder.struct().name("name").version(1).doc("doc")
                        .field("abc", Schema.STRING_SCHEMA)
                        .field("output", Schema.OPTIONAL_STRING_SCHEMA)
                        .build();
        final Struct expectedStruct = new Struct(expectedStructSchema)
                .put("abc", "def")
                .put("output", "");


        final SourceRecord record = new SourceRecord(null, null, "test", 0, initialStructSchema, initialStruct);
        final SourceRecord transformedRecord = smt.apply(record);

        assertEquals(expectedStructSchema, transformedRecord.valueSchema());
        assertEquals(expectedStruct, transformedRecord.value());
    }

    @Test
    public void schemalessDefaultConfigNoElement() {
        final Map<String, Object> props = new HashMap<>();
        smt.configure(props);

        final Map<String, Object> initialMap = new HashMap<>();
        initialMap.put("abc", "def");

        final Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("abc", "def");
        expectedMap.put("output", "");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, null, initialMap);
        final SourceRecord transformedRecord = smt.apply(record);

        assertNull(transformedRecord.valueSchema());
        assertEquals(expectedMap, transformedRecord.value());
    }

    @Test
    public void schemaJoinDifferentTypesNoOptional() {
        final Map<String, Object> props = new HashMap<>();
        props.put("join-fields.fields", Arrays.asList("test_str", "test_int", "test_bool", "test_float"));
        props.put("join-fields.destination", "opt!");
        props.put("join-fields.separator", "_");
        smt.configure(props);

        final Schema initialStructSchema =
                SchemaBuilder.struct().name("name").version(1).doc("doc")
                        .field("test_int", Schema.INT32_SCHEMA)
                        .field("test_str", Schema.STRING_SCHEMA)
                        .field("test_bool", Schema.BOOLEAN_SCHEMA)
                        .field("test_float", Schema.FLOAT64_SCHEMA)
                        .build();
        final Struct initialStruct = new Struct(initialStructSchema)
                .put("test_bool", false)
                .put("test_str", "abc")
                .put("test_int", 123)
                .put("test_float", 456.78);

        final Schema expectedStructSchema =
                SchemaBuilder.struct().name("name").version(1).doc("doc")
                        .field("test_int", Schema.INT32_SCHEMA)
                        .field("test_str", Schema.STRING_SCHEMA)
                        .field("test_bool", Schema.BOOLEAN_SCHEMA)
                        .field("test_float", Schema.FLOAT64_SCHEMA)
                        .field("opt", Schema.STRING_SCHEMA)
                        .build();
        final Struct expectedStruct = new Struct(expectedStructSchema)
                .put("test_str", "abc")
                .put("test_int", 123)
                .put("test_float", 456.78)
                .put("test_bool", false)
                .put("opt", "abc_123_false_456.78");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, initialStructSchema, initialStruct);
        final SourceRecord transformedRecord = smt.apply(record);

        assertEquals(expectedStructSchema, transformedRecord.valueSchema());
        assertEquals(expectedStruct, transformedRecord.value());
    }


    @Test
    public void schemaJoinDifferentTypesOptional() {
        final Map<String, Object> props = new HashMap<>();
        props.put("join-fields.fields", Arrays.asList("test_str", "test_int", "test_bool", "test_float"));
        props.put("join-fields.destination", "opt?");
        props.put("join-fields.separator", "_");
        smt.configure(props);

        final Schema initialStructSchema =
                SchemaBuilder.struct().name("name").version(1).doc("doc")
                        .field("test_str", Schema.STRING_SCHEMA)
                        .field("test_int", Schema.INT32_SCHEMA)
                        .field("test_bool", Schema.BOOLEAN_SCHEMA)
                        .field("test_float", Schema.FLOAT64_SCHEMA)
                        .build();
        final Struct initialStruct = new Struct(initialStructSchema)
                .put("test_str", "abc")
                .put("test_int", 123)
                .put("test_float", 456.78)
                .put("test_bool", false);

        final Schema expectedStructSchema =
                SchemaBuilder.struct().name("name").version(1).doc("doc")
                        .field("test_str", Schema.STRING_SCHEMA)
                        .field("test_int", Schema.INT32_SCHEMA)
                        .field("test_bool", Schema.BOOLEAN_SCHEMA)
                        .field("test_float", Schema.FLOAT64_SCHEMA)
                        .field("opt", Schema.OPTIONAL_STRING_SCHEMA)
                        .build();
        final Struct expectedStruct = new Struct(expectedStructSchema)
                .put("test_str", "abc")
                .put("test_int", 123)
                .put("test_float", 456.78)
                .put("test_bool", false)
                .put("opt", "abc_123_false_456.78");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, initialStructSchema, initialStruct);
        final SourceRecord transformedRecord = smt.apply(record);

        assertEquals(expectedStructSchema, transformedRecord.valueSchema());
        assertEquals(expectedStruct, transformedRecord.value());
    }


    @Test
    public void schemalessJoinDifferentTypes() {
        final Map<String, Object> props = new HashMap<>();
        props.put("join-fields.fields", Arrays.asList("test_str", "test_int", "test_bool", "test_float"));
        props.put("join-fields.destination", "opt");
        props.put("join-fields.separator", "_");
        smt.configure(props);

        final Map<String, Object> initialMap = new HashMap<>();
        initialMap.put("test_str", "abc");
        initialMap.put("test_int", 123);
        initialMap.put("test_float", 456.78);
        initialMap.put("test_bool", false);

        final Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("test_str", "abc");
        expectedMap.put("test_int", 123);
        expectedMap.put("test_float", 456.78);
        expectedMap.put("test_bool", false);
        expectedMap.put("opt", "abc_123_false_456.78");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, null, initialMap);
        final SourceRecord transformedRecord = smt.apply(record);

        assertNull(transformedRecord.valueSchema());
        assertEquals(expectedMap, transformedRecord.value());
    }
}
