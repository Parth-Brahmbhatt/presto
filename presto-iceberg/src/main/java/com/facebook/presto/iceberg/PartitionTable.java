/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.iceberg;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableMap;
import com.netflix.iceberg.StructLike;
import com.netflix.iceberg.types.Conversions;
import com.netflix.iceberg.types.Type;

import java.nio.ByteBuffer;
import java.util.Map;

public class PartitionTable implements SystemTable
{

    @Override
    public Distribution getDistribution()
    {
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return null;
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        return null;
    }

    static class Partition {
        private StructLike values;
        private long recordCount;
        private long fileCount;
        private long size;
        private Map<Integer, Comparable> minValues;
        private Map<Integer, Comparable> maxValues;
        private Map<Integer, Long> nullCounts;

        public Partition(StructLike values, long recordCount, long size, Map<Integer, Comparable> minValues, Map<Integer, Comparable> maxValues, Map<Integer, Long> nullCounts)
        {
            this.values = values;
            this.recordCount = recordCount;
            this.fileCount = 1;
            this.size = size;
            this.minValues = minValues;
            this.maxValues = maxValues;
            this.nullCounts = nullCounts;
        }

        public StructLike getValues()
        {
            return values;
        }

        public long getRecordCount()
        {
            return recordCount;
        }

        public long getFileCount()
        {
            return fileCount;
        }

        public long getSize()
        {
            return size;
        }

        public Map<Integer, Comparable> getMinValues()
        {
            return minValues;
        }

        public Map<Integer, Comparable> getMaxValues()
        {
            return maxValues;
        }

        public Map<Integer, Long> getNullCounts()
        {
            return nullCounts;
        }

        public void incrementRecordCount(long count) {
            this.recordCount = this.recordCount + count;
        }

        public void incrementFileCount() {
            this.recordCount = this.fileCount + 1;
        }

        public void incrementSize(long numberOfBytes) {
            this.recordCount = this.size + numberOfBytes;
        }

        public void updateMin(Map<Integer, Comparable> lowerBounds) {
            for (Map.Entry<Integer, Comparable> entry : lowerBounds.entrySet()) {
                final Comparable comparable = minValues.get(entry.getKey());
                if(comparable == null || comparable.compareTo(entry.getValue()) < 0) {
                    minValues.put(entry.getKey(), entry.getValue());
                }
            }
        }

        public void updateMax(Map<Integer, Comparable> lowerBounds) {
            for (Map.Entry<Integer, Comparable> entry : lowerBounds.entrySet()) {
                final Comparable comparable = maxValues.get(entry.getKey());
                if(comparable == null || comparable.compareTo(entry.getValue()) > 0) {
                    maxValues.put(entry.getKey(), entry.getValue());
                }
            }
        }

        public void updateNullCount(Map<Integer, Long> nullCounts) {
            for (Map.Entry<Integer, Long> entry : nullCounts.entrySet()) {
                final Long nullCount = this.nullCounts.getOrDefault(entry.getKey(), 0l);
                nullCounts.put(entry.getKey(), entry.getValue() + nullCount);
            }
        }
    }

    public static Map<Integer, Comparable> toMap(Map<Integer, ByteBuffer> map, Map<Integer, Type> idToTypeMapping) {
        ImmutableMap.Builder<Integer, Comparable> builder = ImmutableMap.builder();
        for (Map.Entry<Integer, ByteBuffer> entry : map.entrySet()) {
            builder.put(entry.getKey(), Conversions.fromByteBuffer(idToTypeMapping.get(entry.getKey()), entry.getValue()));
        }
        return builder.build();
    }
}
