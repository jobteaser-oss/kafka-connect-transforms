/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

public class RemoveNulls<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Remove nulls (0x00).";

    interface ConfigName {}

    public static final ConfigDef CONFIG_DEF = new ConfigDef();

    private static final String PURPOSE = "nulls removal";

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public R apply(R record) {
        final Object updatedKey = updateDependendingOnSchema(record.keySchema(), record.key());
        final Object updatedValue = updateDependendingOnSchema(record.valueSchema(), record.value());

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedKey, record.valueSchema(), updatedValue, record.timestamp());
    }

    private Object updateDependendingOnSchema(Schema schema, Object recordPart) {
        if (schema == null) {
            return update(recordPart);
        } else {
            return update(recordPart, schema);
        }
    }

    private Map<String, Object> update(Object recordPart) {
        final Map<String, Object> value = requireMap(recordPart, PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value.size());

        for (Map.Entry<String, Object> e : value.entrySet()) {
            final String fieldName = e.getKey();
            final Object fieldContent = e.getValue();
            updatedValue.put(fieldName, removeNulls(fieldContent));
        }

        return updatedValue;
    }

    private Struct update(Object recordPart, Schema schema) {
        final Struct value = requireStruct(recordPart, PURPOSE);

        final Struct updatedValue = new Struct(schema);

        for (Field field : schema.fields()) {
            final String fieldName = field.name();
            final Object fieldContent = value.get(fieldName);
            updatedValue.put(fieldName, removeNulls(fieldContent));
        }

        return updatedValue;
    }

    private Object removeNulls(Object content) {
        if (content instanceof String) {
            return content.toString().replaceAll("\u0000", "");
        } else {
            return content;
        }
    }

    @Override
    public void close() {}
}
