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

public class IntKeysHashSet extends it.unimi.dsi.fastutil.ints.IntOpenHashSet
        implements HashSet {

    public IntKeysHashSet() { }

    public void insert(InternalRow row) {
        this.add(row.getInt(0));
    }

    public void ifNotExistsInsert(InternalRow row, HashSet diffSet) {
        int key = row.getInt(0);
        if (!this.contains(key))
            ((IntKeysHashSet)diffSet).add(key);
    }

    public HashSet union(HashSet other) {
        super.addAll((IntKeysHashSet)other);
        return this;
    }
}
