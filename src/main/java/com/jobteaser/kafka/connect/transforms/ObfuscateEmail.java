package com.jobteaser.kafka.connect.transforms;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class ObfuscateEmail<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Obfuscate first part of email address field using MD5 algorithm";

    private interface ConfigName {
        String EMAIL_FIELD_NAME = "email.field.name";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    ConfigName.EMAIL_FIELD_NAME,
                    ConfigDef.Type.STRING,
                    "email",
                    ConfigDef.Importance.HIGH,
                    "Email field to obfuscate"
            );

    private static final String PURPOSE = "obfuscate email from record";

    public static final Pattern PATTERN = Pattern.compile("^([\\w-.]+)@([\\w-]+\\.+[\\w-]{2,4})$");

    private String fieldName;


    public void configure(Map<String, ?> props) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString("email.field.name");
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
            updatedValue.put(fieldName, obfuscate(updatedValue.get(fieldName).toString()));
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
                updatedValue.put(fName, obfuscate(fContent.toString()));
            } else {
                updatedValue.put(fName, fContent);
            }
        }

        return newRecord(record, schema, updatedValue);
    }

    private String obfuscate(String str) {
        final Matcher matcher = PATTERN.matcher(str);

        if (!matcher.matches()) {
            return str;
        }

        return String.format("%s@%s", DigestUtils.md5Hex(matcher.group(1)), matcher.group(2));
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {}

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends ObfuscateEmail<R> {

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

    public static class Value<R extends ConnectRecord<R>> extends ObfuscateEmail<R> {

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
