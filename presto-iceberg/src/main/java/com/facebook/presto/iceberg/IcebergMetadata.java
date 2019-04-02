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

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.HiveTypeTranslator;
import com.facebook.presto.hive.HiveWrittenPartitions;
import com.facebook.presto.hive.LocationHandle;
import com.facebook.presto.hive.LocationService;
import com.facebook.presto.hive.TransactionalMetadata;
import com.facebook.presto.hive.TypeTranslator;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.iceberg.type.TypeConveter;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableLayoutResult;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.block.ArrayBlockBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.RowBlockBuilder;
import com.facebook.presto.spi.block.SingleRowBlockWriter;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.netflix.iceberg.AppendFiles;
import com.netflix.iceberg.BaseMetastoreTables;
import com.netflix.iceberg.DataFile;
import com.netflix.iceberg.DataFiles;
import com.netflix.iceberg.FileFormat;
import com.netflix.iceberg.FileScanTask;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.PartitionSpecParser;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.SchemaParser;
import com.netflix.iceberg.StructLike;
import com.netflix.iceberg.TableScan;
import com.netflix.iceberg.Transaction;
import com.netflix.iceberg.hadoop.HadoopInputFile;
import com.netflix.iceberg.io.CloseableIterable;
import com.netflix.iceberg.types.Types;
import com.netflix.iceberg.util.StructLikeWrapper;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.HiveTableProperties.getPartitionedBy;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveUtil.schemaTableName;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.netflix.iceberg.types.Types.NestedField.optional;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class IcebergMetadata
        implements ConnectorMetadata, TransactionalMetadata
{
    private static final String SCHEMA_PROPERTY = "schema";
    private static final String PARTITION_SPEC_PROPERTY = "partition_spec";
    private static final String TABLE_PROPERTIES = "table_properties";
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final SemiTransactionalHiveMetastore metastore;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private IcebergConfig icebergConfig;
    private IcebergUtil icebergUtil;
    private Transaction transaction;
    private final LocationService locationService;
    private static final TypeTranslator hiveTypeTranslator = new HiveTypeTranslator();

    public IcebergMetadata(
            SemiTransactionalHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> jsonCodec,
            IcebergConfig icebergConfig,
            IcebergUtil icebergUtil, LocationService locationService)
    {
        this.hdfsEnvironment = hdfsEnvironment;
        this.typeManager = typeManager;
        this.metastore = metastore;
        this.jsonCodec = jsonCodec;
        this.icebergConfig = icebergConfig;
        this.icebergUtil = icebergUtil;
        this.locationService = locationService;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases();
    }

    @Override
    public IcebergTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        final Optional<Table> table = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table.isPresent()) {
            if (icebergUtil.isIcebergTable(table.get())) {
                return IcebergTableHandle.parse(tableName.getTableName(), tableName.getSchemaName());
            }
            else {
                throw new RuntimeException(String.format("%s is not an iceberg table please query using hive catalog", tableName));
            }
        }
        return null;
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        IcebergTableHandle sourceTableHandle = IcebergTableHandle.parse(tableName.getTableName(), tableName.getSchemaName());
        SchemaTableName sourceTableName = new SchemaTableName(tableName.getSchemaName(), sourceTableHandle.getTableName());

        String schemaName = sourceTableHandle.getSchemaName();
        Map<String, ColumnHandle> columnHandles = getColumnHandles(session, sourceTableHandle);
        List<HiveColumnHandle> partitionColumns = columnHandles.entrySet().stream()
                .filter(e -> ((HiveColumnHandle) e.getValue()).isPartitionKey())
                .map(e -> (HiveColumnHandle) e.getValue())
                .collect(Collectors.toList());

        List<ColumnMetadata> columnMetadatas = partitionColumns.stream()
                .map(columnHandle -> getColumnMetadata(session, sourceTableHandle, columnHandle))
                .collect(Collectors.toList());

        // add the partition metrics related columns.
        List<String> partitionMetrics = ImmutableList.of("record_count", "file_count", "total_size");
        partitionMetrics.stream().forEach(metric -> columnMetadatas.add(new ColumnMetadata(metric, BIGINT)));

        // add the min stats, max stats, null count stats
        List<String> columnMetrics = ImmutableList.of("null_count", "min_values", "max_values");
        ImmutableList<RowType.Field> metricRow = ImmutableList.of(new RowType.Field(Optional.of("column_name"), VarcharType.VARCHAR), new RowType.Field(Optional.of("value"), VarcharType.VARCHAR));
        ArrayType columnMetricType = new ArrayType(RowType.from(metricRow));
        columnMetrics.stream().forEach(metric -> columnMetadatas.add(new ColumnMetadata(metric, columnMetricType)));

        Map<Integer, HiveColumnHandle> fieldIdToColumnHandle =
                IntStream.range(0, partitionColumns.size())
                        .boxed()
                        .collect(Collectors.toMap(identity(), partitionColumns::get));

        partitionMetrics.stream().forEach(metric -> fieldIdToColumnHandle.put(fieldIdToColumnHandle.size(), new HiveColumnHandle(metric, HIVE_LONG, BIGINT.getTypeSignature(), partitionColumns.size(), PARTITION_KEY, Optional.empty())));
        columnMetrics.stream().forEach(metric -> fieldIdToColumnHandle.put(fieldIdToColumnHandle.size(), new HiveColumnHandle(metric, HiveType.toHiveType(hiveTypeTranslator, columnMetricType), columnMetricType.getTypeSignature(), partitionColumns.size(), PARTITION_KEY, Optional.empty())));

        List<Type> partitionColumnTypes = columnMetadatas.stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();


        return Optional.of(new SystemTable()
        {
            @Override
            public Distribution getDistribution()
            {
                return Distribution.SINGLE_COORDINATOR;
            }

            @Override
            public ConnectorTableMetadata getTableMetadata()
            {
                return new ConnectorTableMetadata(sourceTableName, columnMetadatas);
            }

            @Override
            public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
            {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(classLoader);
                    TupleDomain<HiveColumnHandle> predicates = constraint.transform(fieldIdToColumnHandle::get);
                    com.netflix.iceberg.Table icebergTable = icebergUtil.getIcebergTable(icebergConfig.getMetacatCatalogName(), schemaName, sourceTableName.getTableName(), getConfiguration(session, schemaName));
                    final Map<Integer, com.netflix.iceberg.types.Type> idToTypeMapping = icebergUtil.getIdToTypeMapping(icebergTable.schema());
                    Long snapshotId = null;
                    Long snapshotTimestampMills = null;
                    Map<StructLikeWrapper, PartitionTable.Partition> partitions = new HashMap<>();
                    if (sourceTableHandle.getAtId() != null) {
                        if (icebergTable.snapshot(sourceTableHandle.getAtId()) != null) {
                            snapshotId = sourceTableHandle.getAtId();
                        }
                        else {
                            snapshotTimestampMills = sourceTableHandle.getAtId();
                        }
                    }
                    TableScan tableScan = icebergUtil.getTableScan(session, predicates, snapshotId, snapshotTimestampMills, icebergTable);

                    try(CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
                        for (FileScanTask fileScanTask : fileScanTasks) {

                            final DataFile dataFile = fileScanTask.file();
                            final StructLike partitionStruct = dataFile.partition();
                            final StructLikeWrapper partitionWrapper = StructLikeWrapper.wrap(partitionStruct);
                            if (!partitions.containsKey(partitionWrapper)) {
                                PartitionTable.Partition partition = new PartitionTable.Partition(partitionStruct,
                                        dataFile.recordCount(),
                                        dataFile.fileSizeInBytes(),
                                        PartitionTable.toMap(dataFile.lowerBounds(), idToTypeMapping),
                                        PartitionTable.toMap(dataFile.upperBounds(), idToTypeMapping),
                                        dataFile.nullValueCounts());
                                partitions.put(partitionWrapper, partition);
                                continue;
                            }

                            PartitionTable.Partition partition = partitions.get(partitionWrapper);
                            partition.incrementFileCount();
                            partition.incrementRecordCount(dataFile.recordCount());
                            partition.incrementSize(dataFile.fileSizeInBytes());
                            partition.updateMin(PartitionTable.toMap(dataFile.lowerBounds(), idToTypeMapping));
                            partition.updateMax(PartitionTable.toMap(dataFile.upperBounds(), idToTypeMapping));
                            partition.updateNullCount(dataFile.nullValueCounts());
                        }
                    } catch (IOException e) {
                        new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, e);
                    }


                    final List<Class> partitionColumnClass = icebergTable.spec().fields().stream()
                            .map(field -> field.transform().getResultType(icebergTable.schema().findType(field.sourceId())).asPrimitiveType().typeId().javaClass())
                            .collect(Collectors.toList());
                    final ImmutableList.Builder<List<Object>> records = new ImmutableList.Builder();
                    for (PartitionTable.Partition partition : partitions.values()) {
                        ImmutableList.Builder<Object> rowBuilder = ImmutableList.builder();
                        final StructLike values = partition.getValues();
                        // add data for partition columns
                        for (int index = 0 ; index < partitionColumnClass.size() ; index++) {
                            rowBuilder.add(partition.getValues().get(index, partitionColumnClass.get(index)));
                        }

                        // add the top level metrics.
                        rowBuilder.add(partition.getRecordCount());
                        rowBuilder.add(partition.getFileCount());
                        rowBuilder.add(partition.getSize());

                        // add column level metrics
                        rowBuilder.add(getBlock(icebergTable, partition, partition.getNullCounts()));
                        rowBuilder.add(getBlock(icebergTable, partition, partition.getMinValues()));
                        rowBuilder.add(getBlock(icebergTable, partition, partition.getMaxValues()));

                        records.add(rowBuilder.build());
                    }

                    return new InMemoryRecordSet(partitionColumnTypes, records.build()).cursor();
                }
                finally {
                    Thread.currentThread().setContextClassLoader(cl);
                }
            }

            private Block getBlock(com.netflix.iceberg.Table icebergTable, PartitionTable.Partition partition, Map<Integer, ?> map)
            {
                ArrayBlockBuilder blockBuilder = new ArrayBlockBuilder(RowType.from(metricRow), null, map.size());
                for (Map.Entry<Integer, ?> entry : map.entrySet()) {
                    RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) RowType.from(metricRow).createBlockBuilder(null, 1);
                    SingleRowBlockWriter singleRowBlockBuilder = rowBlockBuilder.beginBlockEntry();
                    final String columnName = icebergTable.schema().findColumnName(entry.getKey());
                    singleRowBlockBuilder.writeBytes(Slices.utf8Slice(columnName), 0, Slices.utf8Slice(columnName).length());
                    singleRowBlockBuilder.closeEntry();

                    // TODO tostring will break need to handle different types
                    singleRowBlockBuilder.writeBytes(Slices.utf8Slice(entry.getValue().toString()), 0, Slices.utf8Slice(entry.getValue().toString()).length());
                    singleRowBlockBuilder.closeEntry();

                    rowBlockBuilder.closeEntry();
                    columnMetricType.writeObject(blockBuilder, rowBlockBuilder.build());
                }
                return blockBuilder.build();
            }
        });
    }


    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
            ConnectorTableHandle tbl,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns)
    {
        IcebergTableHandle tableHandle = (IcebergTableHandle) tbl;
        final Map<String, HiveColumnHandle> nameToHiveColumnHandleMap = desiredColumns
                .map(cols -> cols.stream().map(col -> HiveColumnHandle.class.cast(col))
                        .collect(toMap(HiveColumnHandle::getName, identity())))
                .orElse(emptyMap());
        final IcebergTableLayoutHandle icebergTableLayoutHandle = new IcebergTableLayoutHandle(
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.getAtId(),
                constraint.getSummary(),
                nameToHiveColumnHandleMap);
        return ImmutableList.of(new ConnectorTableLayoutResult(new ConnectorTableLayout(icebergTableLayoutHandle), constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        final IcebergTableHandle tbl = (IcebergTableHandle) table;
        return getTableMetadata(tbl.getSchemaName(), tbl.getTableName(), session);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchema)
    {
        final List<String> schemas = optionalSchema.<List<String>>map(ImmutableList::of)
                .orElseGet(metastore::getAllDatabases);
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schema : schemas) {
            final Optional<List<String>> allTables = metastore.getAllTables(schema);
            final List<SchemaTableName> schemaTableNames = allTables
                    .map(tables -> tables.stream().map(table -> new SchemaTableName(schema, table)).collect(toList()))
                    .orElse(EMPTY_LIST);
            tableNames.addAll(schemaTableNames);
        }
        return tableNames.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle tbl = (IcebergTableHandle) tableHandle;
        final Configuration configuration = getConfiguration(session, tbl.getSchemaName());
        final com.netflix.iceberg.Table icebergTable = icebergUtil.getIcebergTable(icebergConfig.getMetacatCatalogName(), tbl.getSchemaName(), tbl.getTableName(), configuration);
        final List<HiveColumnHandle> columns = icebergUtil.getColumns(icebergTable.schema(), icebergTable.spec(), typeManager);
        // We use linked hashmap to preserve column order which is required for system table to work.
        Map<String, ColumnHandle> columnHandleMap = new LinkedHashMap<>();
        for (HiveColumnHandle column : columns) {
            columnHandleMap.put(column.getName(), column);
        }

        return columnHandleMap;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        final HiveColumnHandle column = (HiveColumnHandle) columnHandle;
        return new ColumnMetadata(column.getName(), typeManager.getType(column.getTypeSignature()));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        // TODO we need to query the metastore to both check for existance and to get all tables with matching prefix
        requireNonNull(prefix, "prefix is null");
        final Configuration configuration = getConfiguration(session, prefix.getSchemaName());
        final com.netflix.iceberg.Table icebergTable = icebergUtil.getIcebergTable(icebergConfig.getMetacatCatalogName(), prefix.getSchemaName(), prefix.getTableName(), configuration);
        final List<ColumnMetadata> columnMetadatas = getColumnMetadatas(icebergTable);
        return ImmutableMap.<SchemaTableName, List<ColumnMetadata>>builder().put(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()), columnMetadatas).build();
    }

    /**
     * Get statistics for table for given filtering constraint.
     */
    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint<ColumnHandle> constraint)
    {
        return TableStatistics.empty();
    }

    /**
     * Creates a table using the specified table metadata.
     */
    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        final List<ColumnMetadata> columns = tableMetadata.getColumns();
        Schema schema = new Schema(toIceberg(columns));

        final PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        partitionedBy.forEach(builder::identity);
        final PartitionSpec partitionSpec = builder.build();

        final Configuration configuration = getConfiguration(session, schemaName);
        final BaseMetastoreTables hiveTables = icebergUtil.getMetaStoreTables(configuration, icebergConfig.getMetacatCatalogName());

        if (ignoreExisting) {
            final Optional<Table> table = metastore.getTable(schemaName, tableName);
            if (table.isPresent()) {
                return;
            }
        }
        hiveTables.create(schema, partitionSpec, schemaName, tableName);
    }

    /**
     * Get the physical layout for a new table.
     */
    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return empty();
    }

    /**
     * Get the physical layout for a inserting into an existing table.
     */
    @Override
    public Optional<ConnectorNewTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // TODO We need to provide proper partitioning handle and columns here, we need it for bucketing support but for non bucketed tables it is not required.
        return empty();
    }

    /**
     * Begin the atomic creation of a table with data.
     */
    @Override
    public ConnectorOutputTableHandle beginCreateTable(ConnectorSession session,
            ConnectorTableMetadata tableMetadata,
            Optional<ConnectorNewTableLayout> layout)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Schema schema = new Schema(toIceberg(tableMetadata.getColumns()));
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        final PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        partitionedBy.forEach(builder::identity);
        final PartitionSpec partitionSpec = builder.build();

        final Configuration configuration = getConfiguration(session, schemaName);
        final BaseMetastoreTables table = icebergUtil.getMetaStoreTables(configuration, icebergConfig.getMetacatCatalogName());
        //TODO see if there is a way to store this as transaction state.
        this.transaction = table.beginCreate(schema, partitionSpec, schemaName, tableName);
        final List<HiveColumnHandle> hiveColumnHandles = icebergUtil.getColumns(schema, partitionSpec, typeManager);
        LocationHandle locationHandle = locationService.forNewTable(metastore, session, schemaName, tableName);
        Path targetPath = locationService.getQueryWriteInfo(locationHandle).getTargetPath();
        return new IcebergInsertTableHandle(
                schemaName,
                tableName,
                SchemaParser.toJson(transaction.table().schema()),
                PartitionSpecParser.toJson(partitionSpec),
                hiveColumnHandles,
                targetPath.toString(),
                FileFormat.PARQUET);
    }

    /**
     * Finish a table creation with data after the data is written.
     */
    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        return finishInsert(session, (IcebergInsertTableHandle) tableHandle, fragments, computedStatistics);
    }

    /**
     * Begin insert query
     */
    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle tbl = (IcebergTableHandle) tableHandle;
        final Configuration configuration = getConfiguration(session, tbl.getSchemaName());
        final com.netflix.iceberg.Table icebergTable = icebergUtil.getIcebergTable(icebergConfig.getMetacatCatalogName(), tbl.getSchemaName(), tbl.getTableName(), configuration);
        this.transaction = icebergTable.newTransaction();
        String location = icebergTable.location();
        final List<HiveColumnHandle> columns = icebergUtil.getColumns(icebergTable.schema(), icebergTable.spec(), typeManager);
        return new IcebergInsertTableHandle(
                tbl.getSchemaName(),
                tbl.getTableName(),
                SchemaParser.toJson(icebergTable.schema()),
                PartitionSpecParser.toJson(icebergTable.spec()),
                columns,
                icebergUtil.getDataPath(location),
                icebergUtil.getFileFormat(icebergTable));
    }

    /**
     * Finish insert query
     */
    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        final List<CommitTaskData> commitTasks = fragments.stream().map(slice -> jsonCodec.fromJson(slice.getBytes())).collect(toList());
        IcebergInsertTableHandle icebergTable = (IcebergInsertTableHandle) insertHandle;
        final com.netflix.iceberg.types.Type[] partitionColumnTypes = icebergTable.getInputColumns().stream()
                .filter(col -> col.isPartitionKey())
                .map(col -> typeManager.getType(col.getTypeSignature()))
                .map(type -> TypeConveter.convert(type))
                .toArray(com.netflix.iceberg.types.Type[]::new);
        final AppendFiles appendFiles = transaction.newFastAppend();
        final HdfsEnvironment.HdfsContext hdfsContext = new HdfsEnvironment.HdfsContext(session, icebergTable.getSchemaName(), icebergTable.getTableName());
        final Configuration configuration = hdfsEnvironment.getConfiguration(hdfsContext, new Path(icebergTable.getFilePrefix()));
        for (CommitTaskData commitTaskData : commitTasks) {
            final DataFiles.Builder builder;
            builder = DataFiles.builder(transaction.table().spec())
                    .withInputFile(HadoopInputFile.fromLocation(commitTaskData.getPath(), configuration))
                    .withFormat(icebergTable.getFileFormat())
                    .withMetrics(MetricsParser.fromJson(commitTaskData.getMetricsJson()));

            if (!transaction.table().spec().fields().isEmpty()) {
                builder.withPartition(PartitionData.fromJson(commitTaskData.getPartitionDataJson(), partitionColumnTypes));
            }
            appendFiles.appendFile(builder.build());
        }

        appendFiles.commit();
        transaction.commitTransaction();
        return Optional.of(new HiveWrittenPartitions(commitTasks.stream().map(ct -> ct.getPartitionPath()).collect(toList())));
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableLayoutHandle layoutHandle)
    {
        // TODO this is passed to event stream so we may get wrong metrics if this does not have correct info
        return empty();
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;

        if (!metastore.getTable(handle.getSchemaName(), handle.getTableName()).isPresent()) {
            throw new TableNotFoundException(schemaTableName(tableHandle));
        }
        metastore.dropTable(session, handle.getSchemaName(), handle.getTableName());
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        metastore.renameTable(handle.getSchemaName(), handle.getTableName(), newTableName.getSchemaName(), newTableName.getTableName());
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        IcebergTableHandle handle = (IcebergTableHandle) tableHandle;
        final Configuration configuration = getConfiguration(session, handle.getSchemaName());
        final com.netflix.iceberg.Table icebergTable = icebergUtil.getIcebergTable(icebergConfig.getMetacatCatalogName(), handle.getSchemaName(), handle.getTableName(), configuration);
        icebergTable.updateSchema().addColumn(column.getName(), TypeConveter.convert(column.getType())).commit();
    }

    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        HiveColumnHandle handle = (HiveColumnHandle) column;
        final Configuration configuration = getConfiguration(session, icebergTableHandle.getSchemaName());
        final com.netflix.iceberg.Table icebergTable = icebergUtil.getIcebergTable(icebergConfig.getMetacatCatalogName(), icebergTableHandle.getSchemaName(), icebergTableHandle.getTableName(), configuration);
        icebergTable.updateSchema().deleteColumn(handle.getName()).commit();
    }

    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        IcebergTableHandle icebergTableHandle = (IcebergTableHandle) tableHandle;
        HiveColumnHandle columnHandle = (HiveColumnHandle) source;
        final Configuration configuration = getConfiguration(session, icebergTableHandle.getSchemaName());
        final com.netflix.iceberg.Table icebergTable = icebergUtil.getIcebergTable(icebergConfig.getMetacatCatalogName(), icebergTableHandle.getSchemaName(), icebergTableHandle.getTableName(), configuration);
        icebergTable.updateSchema().renameColumn(columnHandle.getName(), target).commit();
    }

    private ConnectorTableMetadata getTableMetadata(String schema, String tableName, ConnectorSession session)
    {
        Optional<Table> table = metastore.getTable(schema, tableName);
        if (!table.isPresent()) {
            throw new TableNotFoundException(new SchemaTableName(schema, tableName));
        }
        final Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session, schema), new Path("file:///tmp"));

        final com.netflix.iceberg.Table icebergTable = icebergUtil.getIcebergTable(icebergConfig.getMetacatCatalogName(), schema, tableName, configuration);

        final List<ColumnMetadata> columns = getColumnMetadatas(icebergTable);

        final ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(TABLE_PROPERTIES, icebergTable.properties());
        properties.put(SCHEMA_PROPERTY, icebergTable.schema());
        properties.put(PARTITION_SPEC_PROPERTY, icebergTable.spec());

        return new ConnectorTableMetadata(new SchemaTableName(schema, tableName), columns, properties.build(), Optional.empty());
    }

    private List<ColumnMetadata> getColumnMetadatas(com.netflix.iceberg.Table icebergTable)
    {
        final List<ColumnMetadata> columnMetadatas = icebergTable.schema().columns().stream()
                .map(c -> new ColumnMetadata(c.name(), TypeConveter.convert(c.type(), typeManager)))
                .collect(toList());
        return columnMetadatas;
    }

    private List<Types.NestedField> toIceberg(List<ColumnMetadata> columns)
    {
        List<Types.NestedField> icebergColumns = new ArrayList<>();
        for (ColumnMetadata column : columns) {
            if (!column.isHidden()) {
                final String name = column.getName();
                final Type type = column.getType();
                final com.netflix.iceberg.types.Type icebergType = TypeConveter.convert(type);
                icebergColumns.add(optional(icebergColumns.size(), name, icebergType));
            }
        }
        return icebergColumns;
    }

    private Configuration getConfiguration(ConnectorSession session, String schemaName)
    {
        return hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session, schemaName), new Path("file:///tmp"));
    }

    @Override
    public void rollback()
    {
        metastore.rollback();
    }

    @Override
    public void commit()
    {
        metastore.commit();
    }

    public SemiTransactionalHiveMetastore getMetastore()
    {
        return metastore;
    }
}
