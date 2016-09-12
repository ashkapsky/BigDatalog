/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.ucla.cs.wis.bigdatalog.spark.storage.map;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.SparkSqlSerializer;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.KVIterator;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.MemoryLocation;
import scala.reflect.ClassTag$;

/**
 * Unsafe-based HashMap for performing aggregations where the aggregated values are fixed-width.
 *
 * This map supports a maximum of 2 billion keys.
 */
public final class UnsafeFixedWidthMonotonicAggregationMap implements Externalizable {

    /**
     * An empty aggregation buffer, encoded in UnsafeRow format. When inserting a new key into the
     * map, we copy this buffer and use it as the value.
     */
    private byte[] emptyAggregationBuffer;

    private StructType aggregationBufferSchema;

    private StructType groupingKeySchema;

    /**
     * Encodes grouping keys as UnsafeRows.
     */
    private UnsafeProjection groupingKeyProjection;

    /**
     * A hashmap which maps from opaque bytearray keys to bytearray values.
     */
    private edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap map;

    /**
     * Re-used pointer to the current aggregation buffer
     */
    private UnsafeRow currentAggregationBuffer = new UnsafeRow();

    private boolean enablePerfMetrics;

    /**
     * Create a new UnsafeFixedWidthAggregationMap.
     *
     * @param emptyAggregationBuffer  the default value for new keys (a "zero" of the agg. function)
     * @param aggregationBufferSchema the schema of the aggregation buffer, used for row conversion.
     * @param groupingKeySchema       the schema of the grouping key, used for row conversion.
     * @param initialCapacity         the initial capacity of the map (a sizing hint to avoid re-hashing).
     * @param pageSizeBytes           the data page size, in bytes; limits the maximum record size.
     * @param enablePerfMetrics       if true, performance metrics will be recorded (has minor perf impact)
     */
    public UnsafeFixedWidthMonotonicAggregationMap(
            InternalRow emptyAggregationBuffer,
            StructType aggregationBufferSchema,
            StructType groupingKeySchema,
            int initialCapacity,
            long pageSizeBytes,
            boolean enablePerfMetrics) {
        this.aggregationBufferSchema = aggregationBufferSchema;
        this.groupingKeyProjection = UnsafeProjection.create(groupingKeySchema);
        this.groupingKeySchema = groupingKeySchema;
        this.map =
                new edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap(initialCapacity, pageSizeBytes, enablePerfMetrics);
        this.enablePerfMetrics = enablePerfMetrics;

        setInitialAggregationBuffer(emptyAggregationBuffer);

        //final UnsafeProjection valueProjection = UnsafeProjection.create(aggregationBufferSchema);
        //this.emptyAggregationBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();
    }

    public UnsafeFixedWidthMonotonicAggregationMap() { /* For Serialization */ }

    public void setInitialAggregationBuffer(InternalRow emptyAggregationBuffer) {
        // Initialize the buffer for aggregation value
        final UnsafeProjection valueProjection = UnsafeProjection.create(aggregationBufferSchema);
        this.emptyAggregationBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();
    }

    public int numElements() {
        return this.map.numElements();
    }

    public UnsafeRow getAggregationBufferFromUnsafeRow(UnsafeRow unsafeGroupingKeyRow) {
        // Probe our map using the serialized key
        final edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap.Location loc = map.lookup(
                unsafeGroupingKeyRow.getBaseObject(),
                unsafeGroupingKeyRow.getBaseOffset(),
                unsafeGroupingKeyRow.getSizeInBytes());
        if (!loc.isDefined()) {
            // This is the first time that we've seen this grouping key, so we'll insert a copy of the
            // empty aggregation buffer into the map:
            boolean putSucceeded = loc.putNewKey(
                    unsafeGroupingKeyRow.getBaseObject(),
                    unsafeGroupingKeyRow.getBaseOffset(),
                    unsafeGroupingKeyRow.getSizeInBytes(),
                    emptyAggregationBuffer,
                    Platform.BYTE_ARRAY_OFFSET,
                    emptyAggregationBuffer.length
            );
            if (!putSucceeded) {
                return null;
            }
        }

        // Reset the pointer to point to the value that we just stored or looked up:
        final MemoryLocation address = loc.getValueAddress();
        currentAggregationBuffer.pointTo(
                address.getBaseObject(),
                address.getBaseOffset(),
                aggregationBufferSchema.length(),
                loc.getValueLength()
        );
        return currentAggregationBuffer;
    }

    /**
     * Returns an iterator over the keys and values in this map. This uses destructive iterator of
     * BytesToBytesMap. So it is illegal to call any other method on this map after `iterator()` has
     * been called.
     * <p>
     * For efficiency, each call returns the same object.
     */
    public KVIterator<UnsafeRow, UnsafeRow> iterator() {
        return new KVIterator<UnsafeRow, UnsafeRow>() {

            private final edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap.MapIterator mapLocationIterator = map.iterator();
            private final UnsafeRow key = new UnsafeRow();
            private final UnsafeRow value = new UnsafeRow();

            @Override
            public boolean next() {
                if (mapLocationIterator.hasNext()) {
                    final edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap.Location loc = mapLocationIterator.next();
                    final MemoryLocation keyAddress = loc.getKeyAddress();
                    final MemoryLocation valueAddress = loc.getValueAddress();
                    key.pointTo(
                            keyAddress.getBaseObject(),
                            keyAddress.getBaseOffset(),
                            groupingKeySchema.length(),
                            loc.getKeyLength()
                    );
                    value.pointTo(
                            valueAddress.getBaseObject(),
                            valueAddress.getBaseOffset(),
                            aggregationBufferSchema.length(),
                            loc.getValueLength()
                    );
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            public UnsafeRow getKey() {
                return key;
            }

            @Override
            public UnsafeRow getValue() {
                return value;
            }

            @Override
            public void close() {
                // Do nothing.
            }
        };
    }

    /**
     * Return the peak memory used so far, in bytes.
     */
    public long getPeakMemoryUsedBytes() {
        return map.getPeakMemoryUsedBytes();
    }

    /**
     * Free the memory associated with this map. This is idempotent and can be called multiple times.
     */
    public void free() {
        map.free();
    }

    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    /*public void printPerfMetrics() {
        if (!enablePerfMetrics) {
            throw new IllegalStateException("Perf metrics not enabled");
        }
        System.out.println("Average probes per lookup: " + map.getAverageProbesPerLookup());
        System.out.println("Number of hash collisions: " + map.getNumHashCollisions());
        System.out.println("Time spent resizing (ns): " + map.getTimeSpentResizingNs());
        System.out.println("Total memory consumption (bytes): " + map.getTotalMemoryConsumption());
    }*/

    public void readExternal(ObjectInput in) throws java.io.IOException {
        long start = System.currentTimeMillis();
        int nKeys = in.readInt();
        int initialCapacity = nKeys;
        if (initialCapacity == 0)
            initialCapacity = 1024 * 16;

        long pageSizeBytes;
        if (SparkEnv.get() != null)
            pageSizeBytes = SparkEnv.get().memoryManager().pageSizeBytes();
        else
            pageSizeBytes = new SparkConf().getSizeAsBytes("spark.buffer.pageSize", "16m");

        enablePerfMetrics = (in.readInt() == 1);

        int serializedSize = in.readInt();
        byte[] bytes = new byte[serializedSize];
        in.readFully(bytes);
        aggregationBufferSchema = (StructType)SparkSqlSerializer.deserialize(bytes, ClassTag$.MODULE$.apply(StructType.class));

        serializedSize = in.readInt();
        bytes = new byte[serializedSize];
        in.readFully(bytes);
        groupingKeySchema = (StructType)SparkSqlSerializer.deserialize(bytes, ClassTag$.MODULE$.apply(StructType.class));

        groupingKeyProjection = UnsafeProjection.create(groupingKeySchema);

        currentAggregationBuffer = new UnsafeRow();

        map = new edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap(initialCapacity, pageSizeBytes, enablePerfMetrics);

        int i = 0;
        byte[] keyBuffer = new byte[1024];
        byte[] valuesBuffer = new byte[1024];
        while (i < nKeys) {
            int keySize = in.readInt();
            int valuesSize = in.readInt();
            if (keySize > keyBuffer.length)
                keyBuffer = new byte[keySize];

            in.readFully(keyBuffer, 0, keySize);
            if (valuesSize > valuesBuffer.length)
                valuesBuffer = new byte[valuesSize];

            in.readFully(valuesBuffer, 0, valuesSize);

            // put it into binary map
            BytesToBytesMap.Location loc = map.lookup(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize);

            assert (!loc.isDefined()): "Duplicated key found!";

            boolean putSucceeded = loc.putNewKey(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize,
                    valuesBuffer, Platform.BYTE_ARRAY_OFFSET, valuesSize);

            if (!putSucceeded)
                throw new IOException("Could not allocate memory to deserialize BytesToBytesMap");

            i += 1;
        }

        //System.out.println("readExternal took " + (System.currentTimeMillis() - start) + " ms");
    }

    public void writeExternal(ObjectOutput out) throws java.io.IOException {
        long start = System.currentTimeMillis();
        // write out the map to byte array
        out.writeInt(map.numElements());
        if (enablePerfMetrics)
            out.writeInt(1);
        else
            out.writeInt(0);

        byte[] serialized = SparkSqlSerializer.serialize(aggregationBufferSchema, ClassTag$.MODULE$.apply(StructType.class));
        out.writeInt(serialized.length);
        out.write(serialized);

        serialized = SparkSqlSerializer.serialize(groupingKeySchema, ClassTag$.MODULE$.apply(StructType.class));
        out.writeInt(serialized.length);
        out.write(serialized);

        byte[] buffer = new byte[64];

        Iterator<BytesToBytesMap.Location> iter = map.iterator();
        while (iter.hasNext()) {
            BytesToBytesMap.Location loc = iter.next();
            // [key size] [values size] [key bytes] [values bytes]
            out.writeInt(loc.getKeyLength());
            out.writeInt(loc.getValueLength());
            write(buffer, loc.getKeyAddress(), loc.getKeyLength(), out);
            write(buffer, loc.getValueAddress(), loc.getValueLength(), out);
        }

        //System.out.println("writeExternal took " + (System.currentTimeMillis() - start) + " ms");
    }

    private void write(byte[] buffer, MemoryLocation addr, int length, ObjectOutput out) throws java.io.IOException {
        if (buffer.length < length)
            buffer = new byte[length];

        Platform.copyMemory(addr.getBaseObject(), addr.getBaseOffset(), buffer, Platform.BYTE_ARRAY_OFFSET, length);
        out.write(buffer, 0, length);
    }
}