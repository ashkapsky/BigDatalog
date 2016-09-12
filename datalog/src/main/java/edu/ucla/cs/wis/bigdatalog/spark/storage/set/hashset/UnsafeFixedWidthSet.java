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

package edu.ucla.cs.wis.bigdatalog.spark.storage.set.hashset;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.memory.MemoryLocation;

import java.util.Iterator;

/**
 * Unsafe-based HashMap for performing aggregations where the aggregated values are fixed-width.
 *
 * This set supports a maximum of 2 billion keys.
 */
public final class UnsafeFixedWidthSet implements HashSet {

    /**
     * An empty aggregation buffer, encoded in UnsafeRow format. When inserting a new key into the
     * set, we copy this buffer and use it as the value.
     */
    //private final byte[] emptyAggregationBuffer;

    //private final StructType aggregationBufferSchema;

    private final StructType groupingKeySchema;

    /**
     * Encodes grouping keys as UnsafeRows.
     */
    private final UnsafeProjection groupingKeyProjection;

    /**
     * A hashmap which maps from opaque bytearray keys to bytearray values.
     */
    private final edu.ucla.cs.wis.bigdatalog.spark.storage.set.hashset.BytesSet set;

    /**
     * Re-used pointer to the current aggregation buffer
     */
    //private final UnsafeRow currentAggregationBuffer = new UnsafeRow();

    private final boolean enablePerfMetrics;

    /**
     * @return true if UnsafeFixedWidthAggregationMap supports aggregation buffers with the given
     *         schema, false otherwise.
     */
    /*public static boolean supportsAggregationBufferSchema(StructType schema) {
        for (StructField field: schema.fields()) {
            if (!UnsafeRow.isMutable(field.dataType())) {
                return false;
            }
        }
        return true;
    }*/

    /**
     * Create a new UnsafeFixedWidthAggregationMap.
     *
     * @param groupingKeySchema the schema of the grouping key, used for row conversion.
     * @param initialCapacity the initial capacity of the set (a sizing hint to avoid re-hashing).
     * @param pageSizeBytes the data page size, in bytes; limits the maximum record size.
     * @param enablePerfMetrics if true, performance metrics will be recorded (has minor perf impact)
     */
    public UnsafeFixedWidthSet(
            //InternalRow emptyAggregationBuffer,
            //StructType aggregationBufferSchema,
            StructType groupingKeySchema,
            int initialCapacity,
            long pageSizeBytes,
            boolean enablePerfMetrics) {
        //this.aggregationBufferSchema = aggregationBufferSchema;
        this.groupingKeyProjection = UnsafeProjection.create(groupingKeySchema);
        this.groupingKeySchema = groupingKeySchema;
        this.set =
                new edu.ucla.cs.wis.bigdatalog.spark.storage.set.hashset.BytesSet(initialCapacity, pageSizeBytes, enablePerfMetrics);
        this.enablePerfMetrics = enablePerfMetrics;

        // Initialize the buffer for aggregation value
        //final UnsafeProjection valueProjection = UnsafeProjection.create(aggregationBufferSchema);
        //this.emptyAggregationBuffer = valueProjection.apply(emptyAggregationBuffer).getBytes();
    }

    public int size() {
        return this.set.numElements();
    }

    /*public boolean exists(InternalRow keyRow) {
        UnsafeRow unsafeGroupingKeyRow = (UnsafeRow)keyRow;
        // Probe our set using the serialized key
        final edu.ucla.cs.wis.bigdatalog.spark.storage.set.hashset.BytesSet.Location loc = set.lookup(
                unsafeGroupingKeyRow.getBaseObject(),
                unsafeGroupingKeyRow.getBaseOffset(),
                unsafeGroupingKeyRow.getSizeInBytes());
        return loc.isDefined();
    }*/

    public void insert(InternalRow keyRow) {
        UnsafeRow unsafeGroupingKeyRow = (UnsafeRow)keyRow;
        // Probe our set using the serialized key
        final edu.ucla.cs.wis.bigdatalog.spark.storage.set.hashset.BytesSet.Location loc = set.lookup(
                unsafeGroupingKeyRow.getBaseObject(),
                unsafeGroupingKeyRow.getBaseOffset(),
                unsafeGroupingKeyRow.getSizeInBytes());

        if (!loc.isDefined()) {
            // This is the first time that we've seen this grouping key, so we'll insert a copy of the
            // empty aggregation buffer into the set:
            boolean putSucceeded = loc.putNewKey(
                    unsafeGroupingKeyRow.getBaseObject(),
                    unsafeGroupingKeyRow.getBaseOffset(),
                    unsafeGroupingKeyRow.getSizeInBytes());

            if (!putSucceeded)
                throw new RuntimeException("Not enough memory to copy value " + unsafeGroupingKeyRow.toString() + " into set.");
            //System.out.println("Inserted " + unsafeGroupingKeyRow + " into set. # entries: " + set.numElements());
        }
    }

    public void ifNotExistsInsert(InternalRow keyRow, HashSet diffSet) {
        UnsafeRow unsafeGroupingKeyRow = (UnsafeRow)keyRow;
        // Probe our set using the serialized key
        final edu.ucla.cs.wis.bigdatalog.spark.storage.set.hashset.BytesSet.Location loc = set.lookup(
                unsafeGroupingKeyRow.getBaseObject(),
                unsafeGroupingKeyRow.getBaseOffset(),
                unsafeGroupingKeyRow.getSizeInBytes());
        if(!loc.isDefined())
            diffSet.insert(keyRow);
    }

    public HashSet union(HashSet other) {
        Iterator<InternalRow> otherIter = ((UnsafeFixedWidthSet)other).iterator();
        while (otherIter.hasNext())
            this.insert(otherIter.next());
        return this;
    }


    /**
         * Return the aggregation buffer for the current group. For efficiency, all calls to this method
         * return the same object. If additional memory could not be allocated, then this method will
         * signal an error by returning null.
         */
    /*public UnsafeRow getAggregationBuffer(InternalRow groupingKey) {
        final UnsafeRow unsafeGroupingKeyRow = this.groupingKeyProjection.apply(groupingKey);

        return getAggregationBufferFromUnsafeRow(unsafeGroupingKeyRow);
    }*/

    /*public UnsafeRow getAggregationBufferFromUnsafeRow(UnsafeRow unsafeGroupingKeyRow) {
        // Probe our set using the serialized key
        final edu.ucla.cs.wis.bigdatalog.spark.storage.set.hashset.BytesSet.Location loc = set.lookup(
                unsafeGroupingKeyRow.getBaseObject(),
                unsafeGroupingKeyRow.getBaseOffset(),
                unsafeGroupingKeyRow.getSizeInBytes());
        if (!loc.isDefined()) {
            // This is the first time that we've seen this grouping key, so we'll insert a copy of the
            // empty aggregation buffer into the set:
            boolean putSucceeded = loc.putNewKey(
                    unsafeGroupingKeyRow.getBaseObject(),
                    unsafeGroupingKeyRow.getBaseOffset(),
                    unsafeGroupingKeyRow.getSizeInBytes()//,
                    //emptyAggregationBuffer,
                    //Platform.BYTE_ARRAY_OFFSET,
                    //emptyAggregationBuffer.length
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
    }/*

    /**
     * Returns an iterator over the keys and values in this set. This uses destructive iterator of
     * BytesToBytesMap. So it is illegal to call any other method on this set after `iterator()` has
     * been called.
     *
     * For efficiency, each call returns the same object.
     */
    public Iterator<InternalRow> iterator() {
        return new Iterator<InternalRow>() {

            private final edu.ucla.cs.wis.bigdatalog.spark.storage.set.hashset.BytesSet.SetIterator setLocationIterator = set.iterator();
            private final UnsafeRow key = new UnsafeRow();
            //private final UnsafeRow value = new UnsafeRow();

            @Override
            public boolean hasNext() {
                return setLocationIterator.hasNext();
            }
            /*    if (setLocationIterator.hasNext()) {
                    final edu.ucla.cs.wis.bigdatalog.spark.storage.set.hashset.BytesSet.Location loc = setLocationIterator.next();
                    final MemoryLocation keyAddress = loc.getKeyAddress();
                    //final MemoryLocation valueAddress = loc.getValueAddress();
                    key.pointTo(
                            keyAddress.getBaseObject(),
                            keyAddress.getBaseOffset(),
                            groupingKeySchema.length(),
                            loc.getKeyLength()
                    );
                    System.out.println("next: " + key.toString());
                    /*value.pointTo(
                            valueAddress.getBaseObject(),
                            valueAddress.getBaseOffset(),
                            aggregationBufferSchema.length(),
                            loc.getValueLength()
                    );*
                    return true;
                } else {
                    return false;
                }
            }*/

            @Override
            public UnsafeRow next() {
                final edu.ucla.cs.wis.bigdatalog.spark.storage.set.hashset.BytesSet.Location loc = setLocationIterator.next();
                final MemoryLocation keyAddress = loc.getKeyAddress();
                key.pointTo(
                        keyAddress.getBaseObject(),
                        keyAddress.getBaseOffset(),
                        groupingKeySchema.length(),
                        loc.getKeyLength()
                );

                return key;
            }

            /*@Override
            public UnsafeRow getValue() {
                return value;
            }*/

            @Override
            public void remove() {
                setLocationIterator.remove();
            }

            /*@Override
            public void close() {
                // Do nothing.
            }*/
        };
    }

    /**
     * Return the peak memory used so far, in bytes.
     */
    public long getPeakMemoryUsedBytes() {
        return set.getPeakMemoryUsedBytes();
    }

    /**
     * Free the memory associated with this set. This is idempotent and can be called multiple times.
     */
    public void free() {
        set.free();
    }

    /*
    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    public void printPerfMetrics() {
        if (!enablePerfMetrics) {
            throw new IllegalStateException("Perf metrics not enabled");
        }
        System.out.println("Average probes per lookup: " + set.getAverageProbesPerLookup());
        System.out.println("Number of hash collisions: " + set.getNumHashCollisions());
        System.out.println("Time spent resizing (ns): " + set.getTimeSpentResizingNs());
        System.out.println("Total memory consumption (bytes): " + set.getTotalMemoryConsumption());
    }*/
}