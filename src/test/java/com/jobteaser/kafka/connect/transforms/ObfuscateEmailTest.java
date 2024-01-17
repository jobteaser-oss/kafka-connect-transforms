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

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.*;

public class ObfuscateEmailTest {

    private final ObfuscateEmail<SourceRecord> smt = new ObfuscateEmail.Value<>();

    @AfterEach
    public void tearDown() throws Exception {
        smt.close();
    }

    @Test
    public void topLevelStructRequired() {
        assertThrows(DataException.class, () -> {
            smt.configure(Collections.singletonMap("email.field.name", "email"));
            smt.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
        });
    }

    @Test
    public void schemaObfuscateEmailFieldTransform() {
        final Map<String, Object> props = new HashMap<>();
        props.put("email.field.name", "email");
        smt.configure(props);

        final Schema simpleStructSchema =
                SchemaBuilder.struct().name("name").version(1).doc("doc")
                        .field("field1", Schema.OPTIONAL_INT64_SCHEMA)
                        .field("email", Schema.STRING_SCHEMA)
                        .build();
        final Struct simpleStruct = new Struct(simpleStructSchema)
                        .put("field1", 42L)
                        .put("email", "abc-def.ghi-jkl123@gmail-test-123abc.fr");


        final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
        final SourceRecord transformedRecord = smt.apply(record);

        assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());
        assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("field1").schema());
        assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("field1"));
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("email").schema());

        final String email = ((Struct) transformedRecord.value()).getString("email");
        assertNotEquals("abc-def.ghi-jkl123@gmail-test-123abc.fr", email);
        assertTrue(ObfuscateEmail.PATTERN.matcher(email).matches());
    }

    @Test
    public void schemaObfuscateEmailFieldNoTransform() {
        final Map<String, Object> props = new HashMap<>();
        props.put("email.field.name", "email");
        smt.configure(props);

        final Schema simpleStructSchema =
                SchemaBuilder.struct().name("name").version(1).doc("doc")
                        .field("field1", Schema.OPTIONAL_INT64_SCHEMA)
                        .field("email", Schema.STRING_SCHEMA)
                        .build();
        final Struct simpleStruct = new Struct(simpleStructSchema)
                .put("field1", 42L)
                .put("email", "not_a_real_email");


        final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
        final SourceRecord transformedRecord = smt.apply(record);

        assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());
        assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("field1").schema());
        assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("field1"));
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("email").schema());
        assertEquals("not_a_real_email", ((Struct) transformedRecord.value()).getString("email"));
    }

    @Test
    public void schemaObfuscateEmailFieldNoEmail() {
        final Map<String, Object> props = new HashMap<>();
        props.put("email.field.name", "email");
        smt.configure(props);

        final Schema simpleStructSchema =
                SchemaBuilder.struct().name("name").version(1).doc("doc")
                        .field("field1", Schema.OPTIONAL_INT64_SCHEMA)
                        .field("field2", Schema.STRING_SCHEMA)
                        .build();
        final Struct simpleStruct = new Struct(simpleStructSchema)
                .put("field1", 42L)
                .put("field2", "abc");


        final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
        final SourceRecord transformedRecord = smt.apply(record);

        assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());
        assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("field1").schema());
        assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("field1"));
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("field2").schema());
        assertEquals("abc", ((Struct) transformedRecord.value()).getString("field2"));
    }

    @Test
    public void schemalessObfuscateEmailFieldTransform() {
        final Map<String, Object> props = new HashMap<>();

        props.put("email.field.name", "email");

        smt.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, Map.ofEntries(entry("field1",42L), entry("email","abc-def.ghi-jkl123@gmail-test-123abc.fr")));

        final SourceRecord transformedRecord = smt.apply(record);
        assertEquals(42L, ((Map<?, ?>) transformedRecord.value()).get("field1"));

        final String email = ((Map<?, ?>) transformedRecord.value()).get("email").toString();
        assertNotEquals("abc-def.ghi-jkl123@gmail-test-123abc.fr", email);
        assertTrue(ObfuscateEmail.PATTERN.matcher(email).matches());
    }

    @Test
    public void schemalessObfuscateEmailFieldNoTransform() {
        final Map<String, Object> props = new HashMap<>();

        props.put("email.field.name", "email");

        smt.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, Map.ofEntries(entry("field1",42L), entry("email","not_a_real_email")));

        final SourceRecord transformedRecord = smt.apply(record);
        assertEquals(42L, ((Map<?, ?>) transformedRecord.value()).get("field1"));
        assertEquals("not_a_real_email", ((Map<?, ?>) transformedRecord.value()).get("email"));
    }

    @Test
    public void schemalessObfuscateEmailFieldNoEmail() {
        final Map<String, Object> props = new HashMap<>();

        props.put("email.field.name", "email");

        smt.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, Map.ofEntries(entry("field1",42L), entry("field2","abc")));

        final SourceRecord transformedRecord = smt.apply(record);
        assertEquals(42L, ((Map<?, ?>) transformedRecord.value()).get("field1"));
        assertEquals("abc", ((Map<?, ?>) transformedRecord.value()).get("field2"));
    }
}
