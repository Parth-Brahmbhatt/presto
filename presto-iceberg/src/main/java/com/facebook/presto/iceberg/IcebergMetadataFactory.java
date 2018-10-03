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

import com.facebook.presto.hive.ForHiveClient;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveConnectorId;
import com.facebook.presto.hive.HiveMetadata;
import com.facebook.presto.hive.HiveMetadataFactory;
import com.facebook.presto.hive.HivePartitionManager;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.LocationService;
import com.facebook.presto.hive.NodeVersion;
import com.facebook.presto.hive.PartitionUpdate;
import com.facebook.presto.hive.TableParameterCodec;
import com.facebook.presto.hive.TypeTranslator;
import com.facebook.presto.hive.metastore.CachingHiveMetastore;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.statistics.MetastoreHiveStatisticsProvider;
import com.facebook.presto.spi.type.TypeManager;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;

public class IcebergMetadataFactory
        extends HiveMetadataFactory
{
    @Override
    public HiveMetadata get()
    {
        SemiTransactionalHiveMetastore metastore = new SemiTransactionalHiveMetastore(
                hdfsEnvironment,
                CachingHiveMetastore.memoizeMetastore(this.metastore, perTransactionCacheMaximumSize), // per-transaction cache
                renameExecution,
                skipDeletionForAlter);

        return new IcebergMetadata(
                connectorId,
                metastore,
                hdfsEnvironment,
                hivePartitionManager,
                timeZone,
                allowCorruptWritesForTesting,
                writesToNonManagedTablesEnabled,
                typeManager,
                locationService,
                tableParameterCodec,
                partitionUpdateCodec,
                typeTranslator,
                prestoVersion,
                new MetastoreHiveStatisticsProvider(typeManager, metastore, timeZone),
                taskCommitCodec,
                domainCompactionThreshold);
    }

    private static final Logger log = Logger.get(IcebergMetadataFactory.class);

    private final String connectorId;
    private final boolean allowCorruptWritesForTesting;
    private final boolean respectTableFormat;
    private final boolean skipDeletionForAlter;
    private final boolean writesToNonManagedTablesEnabled;
    private final HiveStorageFormat defaultStorageFormat;
    private final long perTransactionCacheMaximumSize;
    private final ExtendedHiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager hivePartitionManager;
    private final DateTimeZone timeZone;
    private final int domainCompactionThreshold;
    private final TypeManager typeManager;
    private final LocationService locationService;
    private final TableParameterCodec tableParameterCodec;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final JsonCodec<CommitTaskData> taskCommitCodec;
    private final BoundedExecutor renameExecution;
    private final TypeTranslator typeTranslator;
    private final String prestoVersion;

    @Inject
    @SuppressWarnings("deprecation")
    public IcebergMetadataFactory(
            HiveConnectorId connectorId,
            HiveClientConfig hiveClientConfig,
            ExtendedHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager hivePartitionManager,
            @ForHiveClient ExecutorService executorService,
            TypeManager typeManager,
            LocationService locationService,
            TableParameterCodec tableParameterCodec,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            TypeTranslator typeTranslator,
            NodeVersion nodeVersion,
            JsonCodec<CommitTaskData> commitTaskDataJsonCodec)
    {
        this(connectorId,
                metastore,
                hdfsEnvironment,
                hivePartitionManager,
                hiveClientConfig.getDateTimeZone(),
                hiveClientConfig.getMaxConcurrentFileRenames(),
                hiveClientConfig.getAllowCorruptWritesForTesting(),
                hiveClientConfig.isRespectTableFormat(),
                hiveClientConfig.isSkipDeletionForAlter(),
                hiveClientConfig.getWritesToNonManagedTablesEnabled(),
                hiveClientConfig.getHiveStorageFormat(),
                hiveClientConfig.getPerTransactionMetastoreCacheMaximumSize(),
                hiveClientConfig.getDomainCompactionThreshold(),
                typeManager,
                locationService,
                tableParameterCodec,
                partitionUpdateCodec,
                executorService,
                typeTranslator,
                nodeVersion.toString(),
                commitTaskDataJsonCodec);
    }

    public IcebergMetadataFactory(
            HiveConnectorId connectorId,
            ExtendedHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager hivePartitionManager,
            DateTimeZone timeZone,
            int maxConcurrentFileRenames,
            boolean allowCorruptWritesForTesting,
            boolean respectTableFormat,
            boolean skipDeletionForAlter,
            boolean writesToNonManagedTablesEnabled,
            HiveStorageFormat defaultStorageFormat,
            long perTransactionCacheMaximumSize,
            int domainCompactionThreshold,
            TypeManager typeManager,
            LocationService locationService,
            TableParameterCodec tableParameterCodec,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            ExecutorService executorService,
            TypeTranslator typeTranslator,
            String prestoVersion,
            JsonCodec<CommitTaskData> commitTaskDataJsonCodec)
    {
        super(metastore,
                hdfsEnvironment,
                hivePartitionManager,
                timeZone,
                maxConcurrentFileRenames,
                allowCorruptWritesForTesting,
                skipDeletionForAlter,
                writesToNonManagedTablesEnabled,
                true, //createsOfNonManagedTablesEnabled
                perTransactionCacheMaximumSize,
                Integer.MAX_VALUE, //maxPartitions
                typeManager,
                locationService,
                tableParameterCodec,
                partitionUpdateCodec,
                executorService,
                typeTranslator,
                prestoVersion);
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.allowCorruptWritesForTesting = allowCorruptWritesForTesting;
        this.respectTableFormat = respectTableFormat;
        this.skipDeletionForAlter = skipDeletionForAlter;
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        this.defaultStorageFormat = requireNonNull(defaultStorageFormat, "defaultStorageFormat is null");
        this.perTransactionCacheMaximumSize = perTransactionCacheMaximumSize;
        this.domainCompactionThreshold = domainCompactionThreshold;

        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hivePartitionManager = requireNonNull(hivePartitionManager, "hivePartitionManager is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.tableParameterCodec = requireNonNull(tableParameterCodec, "tableParameterCodec is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.typeTranslator = requireNonNull(typeTranslator, "typeTranslator is null");
        this.prestoVersion = requireNonNull(prestoVersion, "prestoVersion is null");

        if (!allowCorruptWritesForTesting && !timeZone.equals(DateTimeZone.getDefault())) {
            log.warn("Hive writes are disabled. " +
                            "To write data to Hive, your JVM timezone must match the Hive storage timezone. " +
                            "Add -Duser.timezone=%s to your JVM arguments",
                    timeZone.getID());
        }

        renameExecution = new BoundedExecutor(executorService, maxConcurrentFileRenames);
        this.taskCommitCodec = commitTaskDataJsonCodec;
    }
}
