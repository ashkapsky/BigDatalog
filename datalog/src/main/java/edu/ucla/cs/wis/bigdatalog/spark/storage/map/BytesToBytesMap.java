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

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.map.HashMapGrowthStrategy;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.MemoryLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * APS - this is a version of org.apache.spark.unsafe.map.ByteToByteMap that is detached from the memory management infrastructure.
 * It will not ask for memory to be allocated from TaskMemoryManager and will not spill.
 * TODO - hook this up to the storage memory manager.
 *
 * An append-only hash map where keys and values are contiguous regions of bytes.
 *
 * This is backed by a power-of-2-sized hash table, using quadratic probing with triangular numbers,
 * which is guaranteed to exhaust the space.
 *
 * The map can support up to 2^29 keys. If the key cardinality is higher than this, you should
 * probably be using sorting instead of hashing for better cache locality.
 *
 * The key and values under the hood are stored together, in the following format:
 *   Bytes 0 to 4: len(k) (key length in bytes) + len(v) (value length in bytes) + 4
 *   Bytes 4 to 8: len(k)
 *   Bytes 8 to 8 + len(k): key data
 *   Bytes 8 + len(k) to 8 + len(k) + len(v): value data
 *
 * This means that the first four bytes store the entire record (key + value) length. This format
 * is consistent with {@link org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter},
 * so we can pass records from this map directly into the sorter to sort records in place.
 */
public final class BytesToBytesMap implements Serializable {

    private final Logger logger = LoggerFactory.getLogger(edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap.class);

    private static final Murmur3_x86_32 HASHER = new Murmur3_x86_32(0);

    private static final HashMapGrowthStrategy growthStrategy = HashMapGrowthStrategy.DOUBLING;

    // use a memory manager that is not associated with the task
    private BytesToBytesMapMemoryManager memoryManager = null;

    /**
     * A linked list for tracking all allocated data pages so that we can free all of our memory.
     */
    private final LinkedList<MemoryBlock> dataPages = new LinkedList<>();

    /**
     * The data page that will be used to store keys and values for new hashtable entries. When this
     * page becomes full, a new page will be allocated and this pointer will change to point to that
     * new page.
     */
    private MemoryBlock currentPage = null;

    /**
     * Offset into `currentPage` that points to the location where new data can be inserted into
     * the page. This does not incorporate the page's base offset.
     */
    private long pageCursor = 0;

    /**
     * The maximum number of keys that BytesToBytesMap supports. The hash table has to be
     * power-of-2-sized and its backing Java array can contain at most (1 &lt;&lt; 30) elements,
     * since that's the largest power-of-2 that's less than Integer.MAX_VALUE. We need two long array
     * entries per key, giving us a maximum capacity of (1 &lt;&lt; 29).
     */
    @VisibleForTesting
    static final int MAX_CAPACITY = (1 << 29);

    // This choice of page table size and page size means that we can address up to 500 gigabytes
    // of memory.

    /**
     * A single array to store the key and value.
     *
     * Position {@code 2 * i} in the array is used to track a pointer to the key at index {@code i},
     * while position {@code 2 * i + 1} in the array holds key's full 32-bit hashcode.
     */
    @Nullable
    private LongArray longArray;
    // TODO: we're wasting 32 bits of space here; we can probably store fewer bits of the hashcode
    // and exploit word-alignment to use fewer bits to hold the address.  This might let us store
    // only one long per map entry, increasing the chance that this array will fit in cache at the
    // expense of maybe performing more lookups if we have hash collisions.  Say that we stored only
    // 27 bits of the hashcode and 37 bits of the address.  37 bits is enough to address 1 terabyte
    // of RAM given word-alignment.  If we use 13 bits of this for our page table, that gives us a
    // maximum page size of 2^24 * 8 = ~134 megabytes per page. This change will require us to store
    // full base addresses in the page table for off-heap mode so that we can reconstruct the full
    // absolute memory addresses.

    /**
     * Whether or not the longArray can grow. We will not insert more elements if it's false.
     */
    private boolean canGrowArray = true;

    private final double loadFactor;

    /**
     * The size of the data pages that hold key and value data. Map entries cannot span multiple
     * pages, so this limits the maximum entry size.
     */
    private final long pageSizeBytes;

    /**
     * Number of keys defined in the map.
     */
    private int numElements;

    /**
     * The map will be expanded once the number of keys exceeds this threshold.
     */
    private int growthThreshold;

    /**
     * Mask for truncating hashcodes so that they do not exceed the long array's size.
     * This is a strength reduction optimization; we're essentially performing a modulus operation,
     * but doing so with a bitmask because this is a power-of-2-sized hash map.
     */
    private int mask;

    /**
     * Return value of {@link org.apache.spark.unsafe.map.BytesToBytesMap#lookup(Object, long, int)}.
     */
    private final edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap.Location loc;

    private final boolean enablePerfMetrics;

    private long timeSpentResizingNs = 0;

    private long numProbes = 0;

    private long numKeyLookups = 0;

    private long numHashCollisions = 0;

    private long peakMemoryUsedBytes = 0L;

    public BytesToBytesMap(int initialCapacity,
            double loadFactor,
            long pageSizeBytes,
            boolean enablePerfMetrics) {
        this.loadFactor = loadFactor;
        this.loc = new edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap.Location();
        this.pageSizeBytes = pageSizeBytes;
        this.enablePerfMetrics = enablePerfMetrics;
        if (initialCapacity <= 0) {
            throw new IllegalArgumentException("Initial capacity must be greater than 0");
        }
        if (initialCapacity > MAX_CAPACITY) {
            throw new IllegalArgumentException(
                    "Initial capacity " + initialCapacity + " exceeds maximum capacity of " + MAX_CAPACITY);
        }
        if (pageSizeBytes > TaskMemoryManager.MAXIMUM_PAGE_SIZE_BYTES) {
            throw new IllegalArgumentException("Page size " + pageSizeBytes + " cannot exceed " +
                    TaskMemoryManager.MAXIMUM_PAGE_SIZE_BYTES);
        }
        this.memoryManager = new BytesToBytesMapMemoryManager();
        allocate(initialCapacity);
    }

    public BytesToBytesMap(
            int initialCapacity,
            long pageSizeBytes,
            boolean enablePerfMetrics) {
        this(initialCapacity,
                0.70,
                pageSizeBytes,
                enablePerfMetrics);
    }

    /**
     * Returns the number of keys defined in the map.
     */
    public int numElements() { return numElements; }

    public final class MapIterator implements Iterator<edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap.Location> {

        private int numRecords;
        private final edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap.Location loc;

        private MemoryBlock currentPage = null;
        private int recordsInPage = 0;
        private Object pageBaseObject;
        private long offsetInPage;

        private MapIterator(int numRecords, edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap.Location loc) {
            this.numRecords = numRecords;
            this.loc = loc;
        }

        private void advanceToNextPage() {
            synchronized (this) {
                int nextIdx = dataPages.indexOf(currentPage) + 1;

                if (dataPages.size() > nextIdx) {
                    currentPage = dataPages.get(nextIdx);
                    pageBaseObject = currentPage.getBaseObject();
                    offsetInPage = currentPage.getBaseOffset();
                    recordsInPage = Platform.getInt(pageBaseObject, offsetInPage);
                    offsetInPage += 4;
                } else {
                    currentPage = null;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return numRecords > 0;
        }

        @Override
        public edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap.Location next() {
            if (recordsInPage == 0) {
                advanceToNextPage();
            }
            numRecords--;
            if (currentPage != null) {
                int totalLength = Platform.getInt(pageBaseObject, offsetInPage);
                loc.with(currentPage, offsetInPage);
                offsetInPage += 4 + totalLength;
                recordsInPage --;
                return loc;
            }
            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Returns an iterator for iterating over the entries of this map.
     *
     * For efficiency, all calls to `next()` will return the same {@link org.apache.spark.unsafe.map.BytesToBytesMap.Location} object.
     *
     * If any other lookups or operations are performed on this map while iterating over it, including
     * `lookup()`, the behavior of the returned iterator is undefined.
     */
    public edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap.MapIterator iterator() {
        return new edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap.MapIterator(numElements, loc);
    }

    /**
     * Looks up a key, and return a {@link org.apache.spark.unsafe.map.BytesToBytesMap.Location} handle that can be used to test existence
     * and read/write values.
     *
     * This function always return the same {@link org.apache.spark.unsafe.map.BytesToBytesMap.Location} instance to avoid object allocation.
     */
    public edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap.Location lookup(Object keyBase, long keyOffset, int keyLength) {
        safeLookup(keyBase, keyOffset, keyLength, loc);
        return loc;
    }

    /**
     * Looks up a key, and saves the result in provided `loc`.
     *
     * This is a thread-safe version of `lookup`, could be used by multiple threads.
     */
    public void safeLookup(Object keyBase, long keyOffset, int keyLength, edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap.Location loc) {
        assert(longArray != null);

        if (enablePerfMetrics) {
            numKeyLookups++;
        }
        final int hashcode = HASHER.hashUnsafeWords(keyBase, keyOffset, keyLength);
        int pos = hashcode & mask;
        int step = 1;
        while (true) {
            if (enablePerfMetrics) {
                numProbes++;
            }
            if (longArray.get(pos * 2) == 0) {
                // This is a new key.
                loc.with(pos, hashcode, false);
                return;
            } else {
                long stored = longArray.get(pos * 2 + 1);
                if ((int) (stored) == hashcode) {
                    // Full hash code matches.  Let's compare the keys for equality.
                    loc.with(pos, hashcode, true);
                    if (loc.getKeyLength() == keyLength) {
                        final MemoryLocation keyAddress = loc.getKeyAddress();
                        final Object storedkeyBase = keyAddress.getBaseObject();
                        final long storedkeyOffset = keyAddress.getBaseOffset();
                        final boolean areEqual = ByteArrayMethods.arrayEquals(
                                keyBase,
                                keyOffset,
                                storedkeyBase,
                                storedkeyOffset,
                                keyLength
                        );
                        if (areEqual) {
                            return;
                        } else {
                            if (enablePerfMetrics) {
                                numHashCollisions++;
                            }
                        }
                    }
                }
            }
            pos = (pos + step) & mask;
            step++;
        }
    }

    /**
     * Handle returned by {@link org.apache.spark.unsafe.map.BytesToBytesMap#lookup(Object, long, int)} function.
     */
    public final class Location implements Serializable {
        /** An index into the hash map's Long array */
        private int pos;
        /** True if this location points to a position where a key is defined, false otherwise */
        private boolean isDefined;
        /**
         * The hashcode of the most recent key passed to
         * {@link org.apache.spark.unsafe.map.BytesToBytesMap#lookup(Object, long, int)}. Caching this hashcode here allows us to
         * avoid re-hashing the key when storing a value for that key.
         */
        private int keyHashcode;
        private final MemoryLocation keyMemoryLocation = new MemoryLocation();
        private final MemoryLocation valueMemoryLocation = new MemoryLocation();
        private int keyLength;
        private int valueLength;

        /**
         * Memory page containing the record. Only set if created by {@link org.apache.spark.unsafe.map.BytesToBytesMap#iterator()}.
         */
        @Nullable private MemoryBlock memoryPage;

        private void updateAddressesAndSizes(long fullKeyAddress) {
            updateAddressesAndSizes(
                    memoryManager.getPage(fullKeyAddress),
                    memoryManager.getOffsetInPage(fullKeyAddress));
        }

        private void updateAddressesAndSizes(final Object base, final long offset) {
            long position = offset;
            final int totalLength = Platform.getInt(base, position);
            position += 4;
            keyLength = Platform.getInt(base, position);
            position += 4;
            valueLength = totalLength - keyLength - 4;

            keyMemoryLocation.setObjAndOffset(base, position);

            position += keyLength;
            valueMemoryLocation.setObjAndOffset(base, position);
        }

        private edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap.Location with(int pos, int keyHashcode, boolean isDefined) {
            assert(longArray != null);
            this.pos = pos;
            this.isDefined = isDefined;
            this.keyHashcode = keyHashcode;
            if (isDefined) {
                final long fullKeyAddress = longArray.get(pos * 2);
                updateAddressesAndSizes(fullKeyAddress);
            }
            return this;
        }

        private edu.ucla.cs.wis.bigdatalog.spark.storage.map.BytesToBytesMap.Location with(MemoryBlock page, long offsetInPage) {
            this.isDefined = true;
            this.memoryPage = page;
            updateAddressesAndSizes(page.getBaseObject(), offsetInPage);
            return this;
        }

        /**
         * Returns the memory page that contains the current record.
         * This is only valid if this is returned by {@link org.apache.spark.unsafe.map.BytesToBytesMap#iterator()}.
         */
        public MemoryBlock getMemoryPage() {
            return this.memoryPage;
        }

        /**
         * Returns true if the key is defined at this position, and false otherwise.
         */
        public boolean isDefined() {
            return isDefined;
        }

        /**
         * Returns the address of the key defined at this position.
         * This points to the first byte of the key data.
         * Unspecified behavior if the key is not defined.
         * For efficiency reasons, calls to this method always returns the same MemoryLocation object.
         */
        public MemoryLocation getKeyAddress() {
            assert (isDefined);
            return keyMemoryLocation;
        }

        /**
         * Returns the length of the key defined at this position.
         * Unspecified behavior if the key is not defined.
         */
        public int getKeyLength() {
            assert (isDefined);
            return keyLength;
        }

        /**
         * Returns the address of the value defined at this position.
         * This points to the first byte of the value data.
         * Unspecified behavior if the key is not defined.
         * For efficiency reasons, calls to this method always returns the same MemoryLocation object.
         */
        public MemoryLocation getValueAddress() {
            assert (isDefined);
            return valueMemoryLocation;
        }

        /**
         * Returns the length of the value defined at this position.
         * Unspecified behavior if the key is not defined.
         */
        public int getValueLength() {
            assert (isDefined);
            return valueLength;
        }

        /**
         * Store a new key and value. This method may only be called once for a given key; if you want
         * to update the value associated with a key, then you can directly manipulate the bytes stored
         * at the value address. The return value indicates whether the put succeeded or whether it
         * failed because additional memory could not be acquired.
         * <p>
         * It is only valid to call this method immediately after calling `lookup()` using the same key.
         * </p>
         * <p>
         * The key and value must be word-aligned (that is, their sizes must multiples of 8).
         * </p>
         * <p>
         * After calling this method, calls to `get[Key|Value]Address()` and `get[Key|Value]Length`
         * will return information on the data stored by this `putNewKey` call.
         * </p>
         * <p>
         * As an example usage, here's the proper way to store a new key:
         * </p>
         * <pre>
         *   Location loc = map.lookup(keyBase, keyOffset, keyLength);
         *   if (!loc.isDefined()) {
         *     if (!loc.putNewKey(keyBase, keyOffset, keyLength, ...)) {
         *       // handle failure to grow map (by spilling, for example)
         *     }
         *   }
         * </pre>
         * <p>
         * Unspecified behavior if the key is not defined.
         * </p>
         *
         * @return true if the put() was successful and false if the put() failed because memory could
         *         not be acquired.
         */
        public boolean putNewKey(Object keyBase, long keyOffset, int keyLength,
                                 Object valueBase, long valueOffset, int valueLength) {
            assert (!isDefined) : "Can only set value once for a key";
            assert (keyLength % 8 == 0);
            assert (valueLength % 8 == 0);
            assert(longArray != null);


            if (numElements == MAX_CAPACITY
                    // The map could be reused from last spill (because of no enough memory to grow),
                    // then we don't try to grow again if hit the `growthThreshold`.
                    || !canGrowArray && numElements > growthThreshold) {
                return false;
            }

            // Here, we'll copy the data into our data pages. Because we only store a relative offset from
            // the key address instead of storing the absolute address of the value, the key and value
            // must be stored in the same memory page.
            // (8 byte key length) (key) (value)
            final long recordLength = 8 + keyLength + valueLength;
            if (currentPage == null || currentPage.size() - pageCursor < recordLength) {
                if (!acquireNewPage(recordLength + 4L)) {
                    return false;
                }
            }

            // --- Append the key and value data to the current data page --------------------------------
            final Object base = currentPage.getBaseObject();
            long offset = currentPage.getBaseOffset() + pageCursor;
            final long recordOffset = offset;
            Platform.putInt(base, offset, keyLength + valueLength + 4);
            Platform.putInt(base, offset + 4, keyLength);
            offset += 8;
            Platform.copyMemory(keyBase, keyOffset, base, offset, keyLength);
            offset += keyLength;
            Platform.copyMemory(valueBase, valueOffset, base, offset, valueLength);

            // --- Update bookkeeping data structures -----------------------------------------------------
            offset = currentPage.getBaseOffset();
            Platform.putInt(base, offset, Platform.getInt(base, offset) + 1);
            pageCursor += recordLength;
            numElements++;
            final long storedKeyAddress = TaskMemoryManager.encodePageNumberAndOffset(
                    currentPage.pageNumber, recordOffset);
            longArray.set(pos * 2, storedKeyAddress);
            longArray.set(pos * 2 + 1, keyHashcode);
            updateAddressesAndSizes(storedKeyAddress);
            isDefined = true;

            if (numElements > growthThreshold && longArray.size() < MAX_CAPACITY) {
                try {
                    growAndRehash();
                } catch (OutOfMemoryError oom) {
                    canGrowArray = false;
                }
            }
            return true;
        }
    }

    /**
     * Acquire a new page from the memory manager.
     * @return whether there is enough space to allocate the new page.
     */
    private boolean acquireNewPage(long required) {
        try {
            currentPage = allocatePage(required);
        } catch (OutOfMemoryError e) {
            return false;
        }
        dataPages.add(currentPage);
        Platform.putInt(currentPage.getBaseObject(), currentPage.getBaseOffset(), 0);
        pageCursor = 4;
        return true;
    }

    /**
     * Allocate new data structures for this map. When calling this outside of the constructor,
     * make sure to keep references to the old data structures so that you can free them.
     *
     * @param capacity the new map capacity
     */
    private void allocate(int capacity) {
        assert (capacity >= 0);
        capacity = Math.max((int) Math.min(MAX_CAPACITY, ByteArrayMethods.nextPowerOf2(capacity)), 64);
        assert (capacity <= MAX_CAPACITY);
        longArray = allocateArray(capacity * 2);
        longArray.zeroOut();

        this.growthThreshold = (int) (capacity * loadFactor);
        this.mask = capacity - 1;
    }

    /**
     * Free all allocated memory associated with this map, including the storage for keys and values
     * as well as the hash map array itself.
     *
     * This method is idempotent and can be called multiple times.
     */
    public void free() {
        updatePeakMemoryUsed();
        if (longArray != null) {
            freeArray(longArray);
            longArray = null;
        }
        Iterator<MemoryBlock> dataPagesIterator = dataPages.iterator();
        while (dataPagesIterator.hasNext()) {
            MemoryBlock dataPage = dataPagesIterator.next();
            dataPagesIterator.remove();
            freePage(dataPage);
        }
        assert(dataPages.isEmpty());
    }

    public long getPageSizeBytes() {
        return pageSizeBytes;
    }

    /**
     * Returns the total amount of memory, in bytes, consumed by this map's managed structures.
     */
    public long getTotalMemoryConsumption() {
        long totalDataPagesSize = 0L;
        for (MemoryBlock dataPage : dataPages) {
            totalDataPagesSize += dataPage.size();
        }
        return totalDataPagesSize + ((longArray != null) ? longArray.memoryBlock().size() : 0L);
    }

    private void updatePeakMemoryUsed() {
        long mem = getTotalMemoryConsumption();
        if (mem > peakMemoryUsedBytes) {
            peakMemoryUsedBytes = mem;
        }
    }

    /**
     * Return the peak memory used so far, in bytes.
     */
    public long getPeakMemoryUsedBytes() {
        updatePeakMemoryUsed();
        return peakMemoryUsedBytes;
    }

    /**
     * Returns the total amount of time spent resizing this map (in nanoseconds).
     */
    public long getTimeSpentResizingNs() {
        if (!enablePerfMetrics) {
            throw new IllegalStateException();
        }
        return timeSpentResizingNs;
    }

    /**
     * Returns the average number of probes per key lookup.
     */
    public double getAverageProbesPerLookup() {
        if (!enablePerfMetrics) {
            throw new IllegalStateException();
        }
        return (1.0 * numProbes) / numKeyLookups;
    }

    public long getNumHashCollisions() {
        if (!enablePerfMetrics) {
            throw new IllegalStateException();
        }
        return numHashCollisions;
    }

    @VisibleForTesting
    public int getNumDataPages() {
        return dataPages.size();
    }

    /**
     * Returns the underline long[] of longArray.
     */
    public LongArray getArray() {
        assert(longArray != null);
        return longArray;
    }

    /**
     * Reset this map to initialized state.
     */
    public void reset() {
        numElements = 0;
        longArray.zeroOut();

        while (dataPages.size() > 0) {
            MemoryBlock dataPage = dataPages.removeLast();
            freePage(dataPage);
        }
        currentPage = null;
        pageCursor = 0;
    }

    /**
     * Grows the size of the hash table and re-hash everything.
     */
    @VisibleForTesting
    void growAndRehash() {
        assert(longArray != null);

        long resizeStartTime = -1;
        if (enablePerfMetrics) {
            resizeStartTime = System.nanoTime();
        }
        // Store references to the old data structures to be used when we re-hash
        final LongArray oldLongArray = longArray;
        final int oldCapacity = (int) oldLongArray.size() / 2;

        // Allocate the new data structures
        allocate(Math.min(growthStrategy.nextCapacity(oldCapacity), MAX_CAPACITY));

        // Re-mask (we don't recompute the hashcode because we stored all 32 bits of it)
        for (int i = 0; i < oldLongArray.size(); i += 2) {
            final long keyPointer = oldLongArray.get(i);
            if (keyPointer == 0) {
                continue;
            }
            final int hashcode = (int) oldLongArray.get(i + 1);
            int newPos = hashcode & mask;
            int step = 1;
            while (longArray.get(newPos * 2) != 0) {
                newPos = (newPos + step) & mask;
                step++;
            }
            longArray.set(newPos * 2, keyPointer);
            longArray.set(newPos * 2 + 1, hashcode);
        }
        freeArray(oldLongArray);

        if (enablePerfMetrics) {
            timeSpentResizingNs += System.nanoTime() - resizeStartTime;
        }
    }

    protected long used;
    /*****APS - START - Copied from MemoryConsumer*****/
    /**
     * Returns the size of used memory in bytes.
     */
    long getUsed() {
        return used;
    }

    /**
     * Allocates a LongArray of `size`.
     */
    public LongArray allocateArray(long size) {
        long required = size * 8L;
        MemoryBlock page = memoryManager.allocatePage(required);
        if (page == null || page.size() < required) {
            long got = 0;
            if (page != null) {
                got = page.size();
                memoryManager.freePage(page);
            }
            //memoryManager.showMemoryUsage();
            throw new OutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " + got);
        }
        used += required;
        return new LongArray(page);
    }

    /**
     * Frees a LongArray.
     */
    public void freeArray(LongArray array) {
        freePage(array.memoryBlock());
    }

    /**
     * Allocate a memory block with at least `required` bytes.
     *
     * Throws IOException if there is not enough memory.
     *
     * @throws OutOfMemoryError
     */
    protected MemoryBlock allocatePage(long required) {
        MemoryBlock page = memoryManager.allocatePage(Math.max(pageSizeBytes, required));
        if (page == null || page.size() < required) {
            long got = 0;
            if (page != null) {
                got = page.size();
                memoryManager.freePage(page);
            }
            //memoryManager.showMemoryUsage();
            throw new OutOfMemoryError("Unable to acquire " + required + " bytes of memory, got " + got);
        }
        used += page.size();
        return page;
    }

    /**
     * Free a memory block.
     */
    protected void freePage(MemoryBlock page) {
        used -= page.size();
        memoryManager.freePage(page);
    }

    /*****APS - END - Copied from MemoryConsumer*****/
}