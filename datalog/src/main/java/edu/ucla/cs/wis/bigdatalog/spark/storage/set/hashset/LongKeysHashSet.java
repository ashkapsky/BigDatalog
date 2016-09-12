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

import edu.ucla.cs.wis.bigdatalog.spark.SchemaInfo;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.MutableRow;

public class LongKeysHashSet extends it.unimi.dsi.fastutil.longs.LongOpenHashSet
        implements HashSet {

    public SchemaInfo schemaInfo;
    private int numberOfColumns;

    public LongKeysHashSet(SchemaInfo schemaInfo) {
        this.schemaInfo = schemaInfo;
        this.numberOfColumns = schemaInfo.arity();
    }

    public void insert(InternalRow row) {
        this.add(getKeyL(row));
    }

    public void ifNotExistsInsert(InternalRow row, HashSet diffSet) {
        // save time on converting between formats by doing it once
        long key = getKeyL(row);
        if (!this.contains(key))
            ((LongKeysHashSet)diffSet).add(key);
    }

    public HashSet union(HashSet other) {
        super.addAll((LongKeysHashSet)other);
        return this;
    }

    protected long getKeyL(InternalRow row) {
        if (this.numberOfColumns == 2)
            return ((long) row.getInt(0) << 32) | (row.getInt(1) & 0xffffffffL);
        return row.getLong(0);
    }

    /*public void loadRow(long value, MutableRow row) {
        if (this.numberOfColumns == 2) {
            row.setInt(0, (int)(value >> 32));
            row.setInt(1, (int)value);
        } else {
            row.setLong(0, value);
        }
    }*/
}