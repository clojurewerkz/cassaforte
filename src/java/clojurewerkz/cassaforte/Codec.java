/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package clojurewerkz.cassaforte;

import com.datastax.driver.core.DataType;

import org.apache.cassandra.db.marshal.*;

/**
 * Static method to code/decode serialized data given their types.
 */
public class Codec {
    private Codec() {}

    public static <T> AbstractType<T> getCodec(DataType type) {
        return (AbstractType<T>)getCodecInternal(type);
    }

    private static AbstractType getCodecInternal(DataType type) {
        switch (type.getName()) {
            case ASCII:     return AsciiType.instance;
            case BIGINT:    return LongType.instance;
            case BLOB:      return BytesType.instance;
            case BOOLEAN:   return BooleanType.instance;
            case COUNTER:   return CounterColumnType.instance;
            case DECIMAL:   return DecimalType.instance;
            case DOUBLE:    return DoubleType.instance;
            case FLOAT:     return FloatType.instance;
            case INET:      return InetAddressType.instance;
            case INT:       return Int32Type.instance;
            case TEXT:      return UTF8Type.instance;
            case TIMESTAMP: return DateType.instance;
            case UUID:      return UUIDType.instance;
            case VARCHAR:   return UTF8Type.instance;
            case VARINT:    return IntegerType.instance;
            case TIMEUUID:  return TimeUUIDType.instance;
            case LIST:      return ListType.getInstance(getCodec(type.getTypeArguments().get(0)));
            case SET:       return SetType.getInstance(getCodec(type.getTypeArguments().get(0)));
            case MAP:       return MapType.getInstance(getCodec(type.getTypeArguments().get(0)), getCodec(type.getTypeArguments().get(1)));
            default:        throw new RuntimeException("Unknown type");
        }
    }

}
