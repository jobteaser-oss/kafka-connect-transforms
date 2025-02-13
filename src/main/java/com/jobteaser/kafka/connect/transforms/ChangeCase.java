package com.jobteaser.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class ChangeCase<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Change case from lowercase to uppercase or the opposite";

    private interface ConfigName {
        String FIELD = "change-case.field-name";
        String CASE = "change-case.case";
    }

    private enum Case {
        LOWERCASE,
        UPPERCASE,
        UNDEFINED,
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    ConfigName.FIELD,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.HIGH,
                    "Field to change"
            )
            .define(
                    ConfigName.CASE,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.HIGH,
                    "Wanted case"
            );

    private static final String PURPOSE = "change case of some record value";

    private String fieldName;
    private Case wantedCase;

    public void configure(Map<String, ?> props) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.FIELD);
        wantedCase = switch (config.getString(ConfigName.CASE)) {
            case "lowercase" ->  Case.LOWERCASE;
            case "uppercase" ->  Case.UPPERCASE;
            default -> Case.UNDEFINED;
        };
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        if (value.containsKey(fieldName)) {
            final Map<String, Object> updatedValue = new HashMap<>(value);
            updatedValue.put(fieldName, applyChangeCase(value.get(fieldName).toString()));
            return newRecord(record, null, updatedValue);
        }
        return record;
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final Schema schema = operatingSchema(record);
        final Struct updatedValue = new Struct(schema);

        for (Field field : schema.fields()) {
            final String fName = field.name();
            final Object fContent = value.get(fName);
            if (fName.equals(fieldName)) {
                updatedValue.put(fName, applyChangeCase(fContent.toString()));
            } else {
                updatedValue.put(fName, fContent);
            }
        }

        return newRecord(record, schema, updatedValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {}

    private String applyChangeCase(String value) {
        return switch (wantedCase) {
            case UPPERCASE -> value.toUpperCase();
            case LOWERCASE -> value.toLowerCase();
            case UNDEFINED -> value;
        };
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends ChangeCase<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends ChangeCase<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
