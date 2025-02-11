package com.jobteaser.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class JoinFields<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Join record fields values into a new field using the provided separator";

    private interface ConfigName {
        String JOINED_FIELDS = "join-fields.fields";
        String SEPARATOR = "join-fields.separator";
        String DESTINATION = "join-fields.destination";
    }

    private static final class DestinationFieldSpec {
        final String name;
        final boolean optional;

        private DestinationFieldSpec(String name, boolean optional) {
            this.name = name;
            this.optional = optional;
        }

        public static DestinationFieldSpec parse(String spec) {
            if (spec == null) return null;
            if (spec.endsWith("?")) {
                return new DestinationFieldSpec(spec.substring(0, spec.length() - 1), true);
            }
            if (spec.endsWith("!")) {
                return new DestinationFieldSpec(spec.substring(0, spec.length() - 1), false);
            }
            return new DestinationFieldSpec(spec, true);
        }
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    ConfigName.JOINED_FIELDS,
                    ConfigDef.Type.LIST,
                    Collections.emptyList(),
                    ConfigDef.Importance.HIGH,
                    "Keys of the fields to join"
            )
            .define(
                    ConfigName.SEPARATOR,
                    ConfigDef.Type.STRING,
                    ".",
                    ConfigDef.Importance.HIGH,
                    "Separator of the fields values"
            )
            .define(
                    ConfigName.DESTINATION,
                    ConfigDef.Type.STRING,
                    "output",
                    ConfigDef.Importance.HIGH,
                    "Destination key of the joined field. Suffix with <code>!</code> to make this a " +
                            "required field, or <code>?</code> to keep it optional. If omitted, it will default to " +
                            "optional."
            );

    private static final String PURPOSE = "join field records into a new field";

    private List<String> fieldKeys;
    private String separator;
    private DestinationFieldSpec destinationSpec;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldKeys = config.getList(ConfigName.JOINED_FIELDS);
        separator = config.getString(ConfigName.SEPARATOR);
        destinationSpec = DestinationFieldSpec.parse(config.getString(ConfigName.DESTINATION));

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value);

        ArrayList<String> destinationValues = new ArrayList<>();

        for (String fieldKey : fieldKeys) {
            if (updatedValue.containsKey(fieldKey)) {
                final Object fieldValue = updatedValue.get(fieldKey);
                destinationValues.add(fieldValue.toString());
            }
        }

        updatedValue.put(destinationSpec.name, String.join(separator, destinationValues));

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        ArrayList<String> destinationValues = new ArrayList<>();

        for (Field field : value.schema().fields()) {
            String fieldName = field.name();
            Object fieldValue = value.get(field);
            if (fieldKeys.contains(fieldName)) {
                destinationValues.add(fieldValue.toString());
            }
            updatedValue.put(fieldName, fieldValue);
        }

        updatedValue.put(destinationSpec.name, String.join(separator, destinationValues));

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        builder.field(
                destinationSpec.name
                , destinationSpec.optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA
        );

        return builder.build();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends JoinFields<R> {

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

    public static class Value<R extends ConnectRecord<R>> extends JoinFields<R> {

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
