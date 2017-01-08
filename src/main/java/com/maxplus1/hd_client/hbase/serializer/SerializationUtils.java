/*
 * Copyright 2011-2013 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.maxplus1.hd_client.hbase.serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class with various serialization-related methods.
 *
 * @author Costin Leau
 */
public abstract class SerializationUtils {

    static final byte[] EMPTY_ARRAY = new byte[0];

    static boolean isEmpty(byte[] data) {
        return (data == null || data.length == 0);
    }

    @SuppressWarnings("unchecked")
    static <T extends Collection<?>> T deserializeValues(Collection<byte[]> rawValues, Class<T> type,
                                                         HbaseSerializer<?> hbaseSerializer) {
        // connection in pipeline/multi mode
        if (rawValues == null) {
            return null;
        }

        Collection<Object> values = (List.class.isAssignableFrom(type) ? new ArrayList<Object>(rawValues.size())
                : new LinkedHashSet<Object>(rawValues.size()));
        for (byte[] bs : rawValues) {
            values.add(hbaseSerializer.deserialize(bs));
        }

        return (T) values;
    }

    @SuppressWarnings("unchecked")
    public static <T> Set<T> deserialize(Set<byte[]> rawValues, HbaseSerializer<T> hbaseSerializer) {
        return deserializeValues(rawValues, Set.class, hbaseSerializer);
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> deserialize(List<byte[]> rawValues, HbaseSerializer<T> hbaseSerializer) {
        return deserializeValues(rawValues, List.class, hbaseSerializer);
    }

    @SuppressWarnings("unchecked")
    public static <T> Collection<T> deserialize(Collection<byte[]> rawValues, HbaseSerializer<T> hbaseSerializer) {
        return deserializeValues(rawValues, List.class, hbaseSerializer);
    }

    public static <T> Map<T, T> deserialize(Map<byte[], byte[]> rawValues, HbaseSerializer<T> hbaseSerializer) {
        if (rawValues == null) {
            return null;
        }
        Map<T, T> ret = new LinkedHashMap<T, T>(rawValues.size());
        for (Map.Entry<byte[], byte[]> entry : rawValues.entrySet()) {
            ret.put(hbaseSerializer.deserialize(entry.getKey()), hbaseSerializer.deserialize(entry.getValue()));
        }
        return ret;
    }

    @SuppressWarnings("unchecked")
    public static <HK, HV> Map<HK, HV> deserialize(Map<byte[], byte[]> rawValues, HbaseSerializer<HK> hashKeySerializer,
                                                   HbaseSerializer<HV> hashValueSerializer) {
        if (rawValues == null) {
            return null;
        }
        Map<HK, HV> map = new LinkedHashMap<HK, HV>(rawValues.size());
        for (Map.Entry<byte[], byte[]> entry : rawValues.entrySet()) {
            // May want to deserialize only key or value
            HK key = hashKeySerializer != null ? (HK) hashKeySerializer.deserialize(entry.getKey()) : (HK) entry.getKey();
            HV value = hashValueSerializer != null ? (HV) hashValueSerializer.deserialize(entry.getValue()) : (HV) entry
                    .getValue();
            map.put(key, value);
        }
        return map;
    }
}