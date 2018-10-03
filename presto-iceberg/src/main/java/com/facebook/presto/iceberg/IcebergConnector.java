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

import com.facebook.presto.hive.HiveMetadata;
import com.facebook.presto.hive.HiveMetadataFactory;
import com.facebook.presto.hive.HiveTransactionHandle;
import com.facebook.presto.hive.HiveTransactionManager;
import com.facebook.presto.spi.SystemTable;
import com.facebook.presto.spi.classloader.ThreadContextClassLoader;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorNodePartitioningProvider;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.connector.classloader.ClassLoaderSafeConnectorMetadata;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_UNCOMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Created by parth on 2/3/18.
 */
public class IcebergConnector
        implements Connector
{
    private static final Logger log = Logger.get(IcebergConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final HiveMetadataFactory metadataFactory;
    private final HiveTransactionManager transactionManager;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;
    private final ConnectorPageSinkProvider pageSinkProvider;
    private final ConnectorNodePartitioningProvider connectorNodePartitioningProvider;
    private final Set<SystemTable> systemTables;
    private final List<PropertyMetadata<?>> sessionProperties;
    private final List<PropertyMetadata<?>> schemaProperties;
    private final List<PropertyMetadata<?>> tableProperties;
    private final ConnectorAccessControl accessControl;
    private final ClassLoader classLoader;

    @Override
    public ConnectorNodePartitioningProvider getNodePartitioningProvider()
    {
        return this.connectorNodePartitioningProvider;
    }

    public IcebergConnector(
            LifeCycleManager lifeCycleManager,
            HiveMetadataFactory metadataFactory,
            HiveTransactionManager transactionManager,
            ConnectorSplitManager splitManager,
            ConnectorPageSourceProvider pageSourceProvider,
            ConnectorPageSinkProvider pageSinkProvider,
            ConnectorNodePartitioningProvider connectorNodePartitioningProvider,
            Set<SystemTable> systemTables,
            List<PropertyMetadata<?>> sessionProperties,
            List<PropertyMetadata<?>> schemaProperties,
            List<PropertyMetadata<?>> tableProperties,
            ConnectorAccessControl accessControl,
            ClassLoader classLoader)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadataFactory = metadataFactory;
        this.transactionManager = transactionManager;
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
        this.pageSinkProvider = requireNonNull(pageSinkProvider, "pageSinkProvider is null");
        this.connectorNodePartitioningProvider = requireNonNull(connectorNodePartitioningProvider, "pageSinkProvider is null");
        this.systemTables = ImmutableSet.copyOf(requireNonNull(systemTables, "systemTables is null"));
        this.sessionProperties = ImmutableList.copyOf(requireNonNull(sessionProperties, "sessionProperties is null"));
        this.schemaProperties = ImmutableList.copyOf(requireNonNull(schemaProperties, "schemaProperties is null"));
        this.tableProperties = ImmutableList.copyOf(requireNonNull(tableProperties, "tableProperties is null"));
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
    {
        ConnectorMetadata metadata = transactionManager.get(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return new ClassLoaderSafeConnectorMetadata(metadata, classLoader);
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }

    @Override
    public Set<SystemTable> getSystemTables()
    {
        return systemTables;
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getSchemaProperties()
    {
        return schemaProperties;
    }

    @Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    @Override
    public ConnectorAccessControl getAccessControl()
    {
        return accessControl;
    }

    @Override
    public boolean isSingleStatementWritesOnly()
    {
        return false;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        checkConnectorSupports(READ_UNCOMMITTED, isolationLevel);
        ConnectorTransactionHandle transaction = new HiveTransactionHandle();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            transactionManager.put(transaction, metadataFactory.get());
        }
        return transaction;
    }

    @Override
    public void commit(ConnectorTransactionHandle transaction)
    {
        ConnectorMetadata metadata = transactionManager.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (metadata instanceof HiveMetadata) {
                ((HiveMetadata) metadata).commit();
            }
        }
    }

    @Override
    public void rollback(ConnectorTransactionHandle transaction)
    {
        ConnectorMetadata metadata = transactionManager.remove(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (metadata instanceof HiveMetadata) {
                ((HiveMetadata) metadata).rollback();
            }
        }
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
