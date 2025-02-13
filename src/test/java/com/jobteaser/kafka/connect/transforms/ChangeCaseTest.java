package com.jobteaser.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ChangeCaseTest {
    private final ChangeCase<SourceRecord> smt = new ChangeCase.Value<>();

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
    public void schemaDefaultConfigShouldChangeNothing() {
        final Map<String, Object> props = new HashMap<>();
        smt.configure(props);

        final Schema initialStructSchema =
                SchemaBuilder.struct().name("name").version(1).doc("doc")
                        .field("abc", Schema.STRING_SCHEMA)
                        .build();
        final Struct initialStruct = new Struct(initialStructSchema)
                .put("abc", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, initialStructSchema, initialStruct);
        final SourceRecord transformedRecord = smt.apply(record);

        assertEquals(initialStructSchema, transformedRecord.valueSchema());
        assertEquals(initialStruct, transformedRecord.value());
    }

    @Test
    public void schemalessDefaultConfigShouldChangeNothing() {
        final Map<String, Object> props = new HashMap<>();
        smt.configure(props);

        final Map<String, Object> initialMap = new HashMap<>();
        initialMap.put("abc", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, null, initialMap);
        final SourceRecord transformedRecord = smt.apply(record);

        assertNull(transformedRecord.valueSchema());
        assertEquals(initialMap, transformedRecord.value());
    }

    @Test
    public void schemaNoCaseProvidedShouldChangeNothing() {
        final Map<String, Object> props = new HashMap<>();
        props.put("change-case.field-name", "abc");
        smt.configure(props);

        final Schema initialStructSchema =
                SchemaBuilder.struct().name("name").version(1).doc("doc")
                        .field("abc", Schema.STRING_SCHEMA)
                        .build();
        final Struct initialStruct = new Struct(initialStructSchema)
                .put("abc", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, initialStructSchema, initialStruct);
        final SourceRecord transformedRecord = smt.apply(record);

        assertEquals(initialStructSchema, transformedRecord.valueSchema());
        assertEquals(initialStruct, transformedRecord.value());
    }

    @Test
    public void schemalessNoCaseProvidedShouldChangeNothing() {
        final Map<String, Object> props = new HashMap<>();
        props.put("change-case.field-name", "abc");
        smt.configure(props);

        final Map<String, Object> initialMap = new HashMap<>();
        initialMap.put("abc", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, null, initialMap);
        final SourceRecord transformedRecord = smt.apply(record);

        assertNull(transformedRecord.valueSchema());
        assertEquals(initialMap, transformedRecord.value());
    }

    @Test
    public void schemaUnknownFieldShouldChangeNothing() {
        final Map<String, Object> props = new HashMap<>();
        props.put("change-case.field-name", "unknown");
        props.put("change-case.case", "uppercase");
        smt.configure(props);

        final Schema initialStructSchema =
                SchemaBuilder.struct().name("name").version(1).doc("doc")
                        .field("abc", Schema.STRING_SCHEMA)
                        .build();
        final Struct initialStruct = new Struct(initialStructSchema)
                .put("abc", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, initialStructSchema, initialStruct);
        final SourceRecord transformedRecord = smt.apply(record);

        assertEquals(initialStructSchema, transformedRecord.valueSchema());
        assertEquals(initialStruct, transformedRecord.value());
    }

    @Test
    public void schemalessUnknownFieldShouldChangeNothing() {
        final Map<String, Object> props = new HashMap<>();
        props.put("change-case.field-name", "unknown");
        props.put("change-case.case", "uppercase");
        smt.configure(props);

        final Map<String, Object> initialMap = new HashMap<>();
        initialMap.put("abc", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, null, initialMap);
        final SourceRecord transformedRecord = smt.apply(record);

        assertNull(transformedRecord.valueSchema());
        assertEquals(initialMap, transformedRecord.value());
    }

    @Test
    public void schemaUnknownCasedShouldChangeNothing() {
        final Map<String, Object> props = new HashMap<>();
        props.put("change-case.field-name", "abc");
        props.put("change-case.case", "unknown");
        smt.configure(props);

        final Schema initialStructSchema =
                SchemaBuilder.struct().name("name").version(1).doc("doc")
                        .field("abc", Schema.STRING_SCHEMA)
                        .build();
        final Struct initialStruct = new Struct(initialStructSchema)
                .put("abc", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, initialStructSchema, initialStruct);
        final SourceRecord transformedRecord = smt.apply(record);

        assertEquals(initialStructSchema, transformedRecord.valueSchema());
        assertEquals(initialStruct, transformedRecord.value());
    }

    @Test
    public void schemalessUnknownCasedShouldChangeNothing() {
        final Map<String, Object> props = new HashMap<>();
        props.put("change-case.field-name", "abc");
        props.put("change-case.case", "unknown");
        smt.configure(props);

        final Map<String, Object> initialMap = new HashMap<>();
        initialMap.put("abc", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, null, initialMap);
        final SourceRecord transformedRecord = smt.apply(record);

        assertNull(transformedRecord.valueSchema());
        assertEquals(initialMap, transformedRecord.value());
    }

    @Test
    public void schemaCorrectConfigWillChangeCaseToUppercase() {
        final Map<String, Object> props = new HashMap<>();
        props.put("change-case.field-name", "abc");
        props.put("change-case.case", "uppercase");
        smt.configure(props);

        final Schema initialStructSchema =
                SchemaBuilder.struct().name("name").version(1).doc("doc")
                        .field("abc", Schema.STRING_SCHEMA)
                        .build();
        final Struct initialStruct = new Struct(initialStructSchema)
                .put("abc", "dEf");
        final Struct expectedStruct = new Struct(initialStructSchema)
                .put("abc", "DEF");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, initialStructSchema, initialStruct);
        final SourceRecord transformedRecord = smt.apply(record);

        assertEquals(initialStructSchema, transformedRecord.valueSchema());
        assertEquals(expectedStruct, transformedRecord.value());
    }

    @Test
    public void schemalessCorrectConfigWillChangeCaseToUppercase() {
        final Map<String, Object> props = new HashMap<>();
        props.put("change-case.field-name", "abc");
        props.put("change-case.case", "uppercase");
        smt.configure(props);

        final Map<String, Object> initialMap = new HashMap<>();
        initialMap.put("abc", "dEf");

        final Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("abc", "DEF");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, null, initialMap);
        final SourceRecord transformedRecord = smt.apply(record);

        assertNull(transformedRecord.valueSchema());
        assertEquals(expectedMap, transformedRecord.value());
    }

    @Test
    public void schemaCorrectConfigWillChangeCaseToLowercase() {
        final Map<String, Object> props = new HashMap<>();
        props.put("change-case.field-name", "abc");
        props.put("change-case.case", "lowercase");
        smt.configure(props);

        final Schema initialStructSchema =
                SchemaBuilder.struct().name("name").version(1).doc("doc")
                        .field("abc", Schema.STRING_SCHEMA)
                        .build();
        final Struct initialStruct = new Struct(initialStructSchema)
                .put("abc", "dEf");
        final Struct expectedStruct = new Struct(initialStructSchema)
                .put("abc", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, initialStructSchema, initialStruct);
        final SourceRecord transformedRecord = smt.apply(record);

        assertEquals(initialStructSchema, transformedRecord.valueSchema());
        assertEquals(expectedStruct, transformedRecord.value());
    }

    @Test
    public void schemalessCorrectConfigWillChangeCaseToLowercase() {
        final Map<String, Object> props = new HashMap<>();
        props.put("change-case.field-name", "abc");
        props.put("change-case.case", "lowercase");
        smt.configure(props);

        final Map<String, Object> initialMap = new HashMap<>();
        initialMap.put("abc", "dEf");

        final Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("abc", "def");

        final SourceRecord record = new SourceRecord(null, null, "test", 0, null, initialMap);
        final SourceRecord transformedRecord = smt.apply(record);

        assertNull(transformedRecord.valueSchema());
        assertEquals(expectedMap, transformedRecord.value());
    }
}
