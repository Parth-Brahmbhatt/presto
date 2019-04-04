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

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.iceberg.type.TypeConveter;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.netflix.iceberg.DataFile;
import com.netflix.iceberg.FileScanTask;
import com.netflix.iceberg.PartitionField;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.StructLike;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.TableScan;
import com.netflix.iceberg.io.CloseableIterable;
import com.netflix.iceberg.types.Comparators;
import com.netflix.iceberg.types.Conversions;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import com.netflix.iceberg.util.StructLikeWrapper;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class PartitionTable
        implements SystemTable
{

    private final IcebergTableHandle tableHandle;
    private final ConnectorSession session;
    private final IcebergUtil icebergUtil;
    private final Configuration configuration;
    private final IcebergConfig icebergConfig;
    private final TypeManager typeManager;
    private Table icebergTable;
    private Map<Integer, Type.PrimitiveType> idToTypeMapping;
    private List<Types.NestedField> nonPartitionPrimitiveColumns;
    private List<com.facebook.presto.spi.type.Type> resultTypes;
    private List<com.facebook.presto.spi.type.RowType> columnMetricTypes;

    // TODO heck as system table calls do not have connector classloader
    private final ClassLoader connectorClassLoader = Thread.currentThread().getContextClassLoader();
    ;

    public PartitionTable(IcebergTableHandle tableHandle, ConnectorSession session, IcebergUtil icebergUtil, Configuration configuration, IcebergConfig icebergConfig, TypeManager typeManager)
    {
        this.tableHandle = tableHandle;
        this.session = session;
        this.icebergUtil = icebergUtil;
        this.configuration = configuration;
        this.icebergConfig = icebergConfig;
        this.typeManager = typeManager;
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        this.icebergTable = icebergUtil.getIcebergTable(icebergConfig.getMetacatCatalogName(), tableHandle.getSchemaName(), tableHandle.getTableName(), configuration);
        this.idToTypeMapping = icebergTable.schema().columns().stream()
                .filter(column -> column.type().isPrimitiveType())
                .collect(Collectors.toMap(Types.NestedField::fieldId, (column) -> column.type().asPrimitiveType()));

        List<Types.NestedField> columns = icebergTable.schema().columns();
        List<PartitionField> partitionFields = icebergTable.spec().fields();

        ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();

        ColumnMetadata partitionColumnsMetadata = getPartitionColumnsMetadata(partitionFields, icebergTable.schema());
        columnMetadataBuilder.add(partitionColumnsMetadata);

        Set<Integer> identityPartitionIds = icebergUtil.getIdentityPartitions(icebergTable.spec()).keySet().stream()
                .map(partitionField -> partitionField.sourceId())
                .collect(Collectors.toSet());

        this.nonPartitionPrimitiveColumns = columns.stream()
                .filter(column -> !identityPartitionIds.contains(column.fieldId()) && column.type().isPrimitiveType())
                .collect(Collectors.toList());

        List<String> perPartitionMetrics = ImmutableList.of("record_count", "file_count", "total_size");
        perPartitionMetrics.stream().forEach(metric -> columnMetadataBuilder.add(new ColumnMetadata(metric, BIGINT)));

        List<ColumnMetadata> columnMetricsMetadata = getColumnMetadata(nonPartitionPrimitiveColumns);
        columnMetadataBuilder.addAll(columnMetricsMetadata);

        this.columnMetricTypes = columnMetricsMetadata.stream().map(m -> (RowType) m.getType()).collect(Collectors.toList());

        ImmutableList<ColumnMetadata> columnMetadata = columnMetadataBuilder.build();
        this.resultTypes = columnMetadata.stream().map(m -> m.getType()).collect(Collectors.toList());
        return new ConnectorTableMetadata(new SchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName()), columnMetadata);
    }

    private final ColumnMetadata getPartitionColumnsMetadata(List<PartitionField> fields, Schema schema)
    {
        List<RowType.Field> partitionFields = fields.stream()
                .map(field -> new RowType.Field(Optional.of(field.name()), TypeConveter.convert(field.transform().getResultType(schema.findType(field.sourceId())), typeManager)))
                .collect(Collectors.toList());
        return new ColumnMetadata("partition", RowType.from(partitionFields));
    }

    private final List<ColumnMetadata> getColumnMetadata(List<Types.NestedField> columns)
    {
        return columns.stream().map(column -> new ColumnMetadata(column.name(),
                RowType.from(
                        ImmutableList.of(
                                new RowType.Field(Optional.of("min"), TypeConveter.convert(column.type(), typeManager)),
                                new RowType.Field(Optional.of("max"), TypeConveter.convert(column.type(), typeManager)),
                                new RowType.Field(Optional.of("nullCount"), BIGINT)))))
                .collect(Collectors.toList());
    }

    @Override
    public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(connectorClassLoader);
            TableScan tableScan = getTableScan(TupleDomain.all()); // TODO: Apply the actual filters.
            Map<StructLikeWrapper, Partition> partitions = getPartitions(tableScan);
            return buildRecordCursor(partitions, icebergTable.spec().fields());
        }
        finally {
            // TODO heck, set the original classloader
            Thread.currentThread().setContextClassLoader(cl);
        }
    }

    private Map<StructLikeWrapper, Partition> getPartitions(TableScan tableScan)
    {
        Map<StructLikeWrapper, Partition> partitions = new HashMap<>();

        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            for (FileScanTask fileScanTask : fileScanTasks) {

                final DataFile dataFile = fileScanTask.file();
                final StructLike partitionStruct = dataFile.partition();
                final StructLikeWrapper partitionWrapper = StructLikeWrapper.wrap(partitionStruct);
                if (!partitions.containsKey(partitionWrapper)) {
                    Partition partition = new Partition(partitionStruct,
                            dataFile.recordCount(),
                            dataFile.fileSizeInBytes(),
                            toMap(dataFile.lowerBounds()),
                            toMap(dataFile.upperBounds()),
                            dataFile.nullValueCounts());
                    partitions.put(partitionWrapper, partition);
                    continue;
                }

                Partition partition = partitions.get(partitionWrapper);
                partition.incrementFileCount();
                partition.incrementRecordCount(dataFile.recordCount());
                partition.incrementSize(dataFile.fileSizeInBytes());
                partition.updateMin(toMap(dataFile.lowerBounds()));
                partition.updateMax(toMap(dataFile.upperBounds()));
                partition.updateNullCount(dataFile.nullValueCounts());
            }
        }
        catch (IOException e) {
            new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, e);
        }
        return partitions;
    }

    private final RecordCursor buildRecordCursor(Map<StructLikeWrapper, Partition> partitions, List<PartitionField> partitionFields)
    {
        final ImmutableList.Builder<List<Object>> records = new ImmutableList.Builder();
        List<Class> partitionColumnClass = partitionClasses(partitionFields);

        for (PartitionTable.Partition partition : partitions.values()) {
            ImmutableList.Builder<Object> rowBuilder = ImmutableList.builder();
            // add data for partition columns
            rowBuilder.add(getPartitionValuesBlock(partition.getValues(), partitionColumnClass));

            // add the top level metrics.
            rowBuilder.add(partition.getRecordCount());
            rowBuilder.add(partition.getFileCount());
            rowBuilder.add(partition.getSize());

            // add column level metrics
            for (int i = 0; i < columnMetricTypes.size(); i++) {
                Integer fieldId = nonPartitionPrimitiveColumns.get(i).fieldId();
                Type.PrimitiveType type = idToTypeMapping.get(fieldId);
                Object min = convert(partition.getMinValues().get(fieldId), type);
                Object max = convert(partition.getMaxValues().get(fieldId), type);
                Long nullCount = partition.getNullCounts().get(fieldId);
                rowBuilder.add(getColumnMetricBlock(columnMetricTypes.get(i), min, max, nullCount));
            }

            records.add(rowBuilder.build());
        }

        return new InMemoryRecordSet(resultTypes, records.build()).cursor();
    }

    private List<Class> partitionClasses(List<PartitionField> partitionFields)
    {
        ImmutableList.Builder<Class> partitionClassesBuilder = ImmutableList.builder();
        for (int i = 0; i < partitionFields.size(); i++) {
            PartitionField partitionField = partitionFields.get(i);
            Type.PrimitiveType sourceType = idToTypeMapping.get(partitionField.sourceId());
            Class<?> javaClass = partitionField.transform().getResultType(sourceType).typeId().javaClass();
            partitionClassesBuilder.add(javaClass);
        }
        return partitionClassesBuilder.build();
    }

    private Block getColumnMetricBlock(RowType columnMetricType, Object min, Object max, Long nullCount)
    {
        BlockBuilder rowBlockBuilder = columnMetricType.createBlockBuilder(null, 1);
        BlockBuilder builder = rowBlockBuilder.beginBlockEntry();
        List<RowType.Field> fields = columnMetricType.getFields();
        TypeUtils.writeNativeValue(fields.get(0).getType(), builder, min);
        TypeUtils.writeNativeValue(fields.get(1).getType(), builder, max);
        TypeUtils.writeNativeValue(fields.get(2).getType(), builder, nullCount);

        rowBlockBuilder.closeEntry();
        return columnMetricType.getObject(rowBlockBuilder, 0);
    }

    private Block getPartitionValuesBlock(StructLike partition, List<Class> partitionClasses)
    {
        com.facebook.presto.spi.type.RowType partitionValuesType = (RowType) resultTypes.get(0);
        BlockBuilder rowBlockBuilder = partitionValuesType.createBlockBuilder(null, 1);
        BlockBuilder builder = rowBlockBuilder.beginBlockEntry();
        List<RowType.Field> fields = partitionValuesType.getFields();

        for (int i = 0; i < fields.size(); i++) {
            TypeUtils.writeNativeValue(fields.get(i).getType(), builder, partition.get(i, partitionClasses.get(i)));
        }

        rowBlockBuilder.closeEntry();
        return partitionValuesType.getObject(rowBlockBuilder, 0);
    }

    private final TableScan getTableScan(TupleDomain<HiveColumnHandle> predicates)
    {
        Long snapshotId = null;
        Long snapshotTimestampMills = null;
        if (tableHandle.getAtId() != null) {
            if (icebergTable.snapshot(tableHandle.getAtId()) != null) {
                snapshotId = tableHandle.getAtId();
            }
            else {
                snapshotTimestampMills = tableHandle.getAtId();
            }
        }
        return icebergUtil.getTableScan(session, predicates, snapshotId, snapshotTimestampMills, icebergTable);
    }

    private Map<Integer, Object> toMap(Map<Integer, ByteBuffer> idToMetricMap)
    {
        ImmutableMap.Builder<Integer, Object> builder = ImmutableMap.builder();
        for (Map.Entry<Integer, ByteBuffer> idToMetricEntry : idToMetricMap.entrySet()) {
            Integer id = idToMetricEntry.getKey();
            Type.PrimitiveType type = idToTypeMapping.get(id);
            builder.put(id, Conversions.fromByteBuffer(type, idToMetricEntry.getValue()));
        }
        return builder.build();
    }

    private static Object convert(Object value, Type type) {
        if (type instanceof Types.StringType) {
            return value.toString();
        } else if (type instanceof Types.BinaryType) {
            ByteBuffer buffer = (ByteBuffer) value;
            return buffer.get(new byte[buffer.remaining()]);
        } else if (type instanceof Types.DecimalType) {
            return value.toString(); //TODO handle decimal properly.
        }
        return value;
    }

    private class Partition
    {
        private StructLike values;
        private long recordCount;
        private long fileCount;
        private long size;
        private Map<Integer, Object> minValues;
        private Map<Integer, Object> maxValues;
        private Map<Integer, Long> nullCounts;

        public Partition(StructLike values, long recordCount, long size, Map<Integer, Object> minValues, Map<Integer, Object> maxValues, Map<Integer, Long> nullCounts)
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

        public Map<Integer, Object> getMinValues()
        {
            return minValues;
        }

        public Map<Integer, Object> getMaxValues()
        {
            return maxValues;
        }

        public Map<Integer, Long> getNullCounts()
        {
            return nullCounts;
        }

        public void incrementRecordCount(long count)
        {
            this.recordCount = this.recordCount + count;
        }

        public void incrementFileCount()
        {
            this.recordCount = this.fileCount + 1;
        }

        public void incrementSize(long numberOfBytes)
        {
            this.recordCount = this.size + numberOfBytes;
        }

        /**
         * The update logic is built with the following rules:
         * bounds is null => if any file has a missing bound for a column, that bound will not be reported
         * bounds is missing id => not reported in Parquet => that bound will not be reported
         * bound value is null => not an expected case
         * bound value is present => this is the normal case and bounds will be reported correctly
         */

        public void updateMin(Map<Integer, Object> lowerBounds)
        {
            Iterator<Map.Entry<Integer, Object>> iterator = minValues.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, Object> entry = iterator.next();
                Comparator<Object> comparator = Comparators.forType(PartitionTable.this.idToTypeMapping.get(entry.getKey()));
                final Object potentialMin = lowerBounds.get(entry.getKey());
                if (potentialMin == null) {
                    // metric is missing so we remove it from
                    iterator.remove();
                }
                else if (comparator.compare(potentialMin, entry.getValue()) > 0) {
                    minValues.put(entry.getKey(), potentialMin);
                }
            }
        }

        public void updateMax(Map<Integer, Object> upperBounds)
        {
            Iterator<Map.Entry<Integer, Object>> iterator = maxValues.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer, Object> entry = iterator.next();
                Comparator<Object> comparator = Comparators.forType(PartitionTable.this.idToTypeMapping.get(entry.getKey()));
                final Object potentialMax = upperBounds.get(entry.getKey());
                if (potentialMax == null) {
                    // metric is missing so we remove it from
                    iterator.remove();
                }
                else if (comparator.compare(potentialMax, entry.getValue()) < 0) {
                    minValues.put(entry.getKey(), potentialMax);
                }
            }
        }

        public void updateNullCount(Map<Integer, Long> nullCounts)
        {
            for (Map.Entry<Integer, Long> entry : nullCounts.entrySet()) {
                final Long nullCount = this.nullCounts.getOrDefault(entry.getKey(), 0l);
                nullCounts.put(entry.getKey(), entry.getValue() + nullCount);
            }
        }
    }
}
