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
package com.facebook.presto.metadata;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.metacat.client.Client;
import com.netflix.metacat.common.dto.TableDto;
import com.netflix.metacat.common.exception.MetacatNotFoundException;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_SCHEMA_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.SCHEMA_NOT_SPECIFIED;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class MetadataUtil
{
    private static final Logger log = Logger.get(MetadataUtil.class);

    private static final String PARTITIONS_TABLE_SUFFIX = "$partitions";

    private MetadataUtil() {}

    public static void checkTableName(String catalogName, Optional<String> schemaName, Optional<String> tableName)
    {
        checkCatalogName(catalogName);
        schemaName.ifPresent(name -> checkLowerCase(name, "schemaName"));
        tableName.ifPresent(name -> checkLowerCase(name, "tableName"));

        checkArgument(schemaName.isPresent() || !tableName.isPresent(), "tableName specified but schemaName is missing");
    }

    public static String checkCatalogName(String catalogName)
    {
        return checkLowerCase(catalogName, "catalogName");
    }

    public static String checkSchemaName(String schemaName)
    {
        return checkLowerCase(schemaName, "schemaName");
    }

    public static String checkTableName(String tableName)
    {
        return checkLowerCase(tableName, "tableName");
    }

    public static void checkObjectName(String catalogName, String schemaName, String objectName)
    {
        checkLowerCase(catalogName, "catalogName");
        checkLowerCase(schemaName, "schemaName");
        checkLowerCase(objectName, "objectName");
    }

    public static String checkLowerCase(String value, String name)
    {
        if (value == null) {
            throw new NullPointerException(format("%s is null", name));
        }
        checkArgument(value.equals(value.toLowerCase(ENGLISH)), "%s is not lowercase: %s", name, value);
        return value;
    }

    public static ColumnMetadata findColumnMetadata(ConnectorTableMetadata tableMetadata, String columnName)
    {
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            if (columnName.equals(columnMetadata.getName())) {
                return columnMetadata;
            }
        }
        return null;
    }

    public static CatalogSchemaName createCatalogSchemaName(Session session, Node node, Optional<QualifiedName> schema)
    {
        String catalogName = session.getCatalog().orElse(null);
        String schemaName = session.getSchema().orElse(null);

        if (schema.isPresent()) {
            List<String> parts = schema.get().getParts();
            if (parts.size() > 2) {
                throw new SemanticException(INVALID_SCHEMA_NAME, node, "Too many parts in schema name: %s", schema.get());
            }
            if (parts.size() == 2) {
                catalogName = parts.get(0);
            }
            schemaName = schema.get().getSuffix();
        }

        if (catalogName == null) {
            throw new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set");
        }
        if (schemaName == null) {
            throw new SemanticException(SCHEMA_NOT_SPECIFIED, node, "Schema must be specified when session schema is not set");
        }

        return new CatalogSchemaName(catalogName, schemaName);
    }

    public static QualifiedObjectName createQualifiedObjectName(Session session, Node node, QualifiedName name)
    {
        requireNonNull(session, "session is null");
        requireNonNull(name, "name is null");
        if (name.getParts().size() > 3) {
            throw new PrestoException(SYNTAX_ERROR, format("Too many dots in table name: %s", name));
        }

        List<String> parts = Lists.reverse(name.getParts());
        String originalTableName = parts.get(0);
        String schemaName = (parts.size() > 1) ? parts.get(1) : session.getSchema().orElseThrow(() ->
                new SemanticException(SCHEMA_NOT_SPECIFIED, node, "Schema must be specified when session schema is not set"));
        String catalogName = (parts.size() > 2) ? parts.get(2) : session.getCatalog().orElseThrow(() ->
                new SemanticException(CATALOG_NOT_SPECIFIED, node, "Catalog must be specified when session catalog is not set"));

        final String metacatUri = SystemSessionProperties.getMetacatUri(session);
        if (metacatUri != null) {
            final Map<String, String> metacatCatalogMapping = SystemSessionProperties.getMetacatCatalogMapping(session);
            final Map<String, String> icebergCatalogMapping = SystemSessionProperties.getIcebergCatalogMapping(session);
            final Map<String, String> hiveToIcebergCatalogMapping = icebergCatalogMapping.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
            if (metacatCatalogMapping.containsKey(catalogName) && !"information_schema".equalsIgnoreCase(schemaName) &&
                    (hiveToIcebergCatalogMapping.containsKey(catalogName) || icebergCatalogMapping.containsKey(catalogName))) {
                final String stack = System.getenv("stack");
                Client client = Client.builder()
                        .withHost(metacatUri)
                        .withDataTypeContext("hive")
                        .withUserName("presto-iceberg-checker")
                        .withClientAppName("presto-" + stack)
                        .build();
                try {
                    final String metacatCatalogName = metacatCatalogMapping.get(catalogName);

                    String tableName = originalTableName;
                    //TODO HANDLE THE PARTITIONING and @ shit here
                    if (isPartitionsSystemTable(originalTableName)) {
                        tableName = getSourceTableNameForPartitionsTable(originalTableName);
                    }

                    final TableDto table = client.getApi().getTable(metacatCatalogName, schemaName, tableName, true, true, false);
                    final Map<String, String> metadata = table.getMetadata();
                    if (metadata.containsKey("table_type") && metadata.get("table_type").equalsIgnoreCase("iceberg")) {
                        if (hiveToIcebergCatalogMapping.containsKey(catalogName)) {
                            log.info("Rewriting the catalog from %s to %s ", catalogName, hiveToIcebergCatalogMapping.get(catalogName));
                            return new QualifiedObjectName(hiveToIcebergCatalogMapping.get(catalogName), schemaName, originalTableName);
                        }
                    }
                    else {
                        if (icebergCatalogMapping.containsKey(catalogName)) {
                            log.info("Rewriting the catalog from %s to %s ", catalogName, icebergCatalogMapping.get(catalogName));
                            return new QualifiedObjectName(icebergCatalogMapping.get(catalogName), schemaName, originalTableName);
                        }
                    }
                }
                catch (MetacatNotFoundException e) {
                    log.info("Ignoring the exception, let normal processing handle it correctly", e);
                }
            }
        }
        return new QualifiedObjectName(catalogName, schemaName, originalTableName);
    }

    public static boolean isPartitionsSystemTable(String tableName)
    {
        return tableName.endsWith(PARTITIONS_TABLE_SUFFIX) && tableName.length() > PARTITIONS_TABLE_SUFFIX.length();
    }

    public static String getSourceTableNameForPartitionsTable(String tableName)
    {
        checkArgument(isPartitionsSystemTable(tableName), "not a partitions table name");
        return tableName.substring(0, tableName.length() - PARTITIONS_TABLE_SUFFIX.length());
    }

    public static QualifiedName createQualifiedName(QualifiedObjectName name)
    {
        return QualifiedName.of(name.getCatalogName(), name.getSchemaName(), name.getObjectName());
    }

    public static boolean tableExists(Metadata metadata, Session session, String table)
    {
        if (!session.getCatalog().isPresent() || !session.getSchema().isPresent()) {
            return false;
        }
        QualifiedObjectName name = new QualifiedObjectName(session.getCatalog().get(), session.getSchema().get(), table);
        return metadata.getTableHandle(session, name).isPresent();
    }

    public static class SchemaMetadataBuilder
    {
        public static SchemaMetadataBuilder schemaMetadataBuilder()
        {
            return new SchemaMetadataBuilder();
        }

        private final ImmutableMap.Builder<SchemaTableName, ConnectorTableMetadata> tables = ImmutableMap.builder();

        public SchemaMetadataBuilder table(ConnectorTableMetadata tableMetadata)
        {
            tables.put(tableMetadata.getTable(), tableMetadata);
            return this;
        }

        public ImmutableMap<SchemaTableName, ConnectorTableMetadata> build()
        {
            return tables.build();
        }
    }

    public static class TableMetadataBuilder
    {
        public static TableMetadataBuilder tableMetadataBuilder(String schemaName, String tableName)
        {
            return new TableMetadataBuilder(new SchemaTableName(schemaName, tableName));
        }

        public static TableMetadataBuilder tableMetadataBuilder(SchemaTableName tableName)
        {
            return new TableMetadataBuilder(tableName);
        }

        private final SchemaTableName tableName;
        private final ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        private final ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        private final Optional<String> comment;

        private TableMetadataBuilder(SchemaTableName tableName)
        {
            this(tableName, Optional.empty());
        }

        private TableMetadataBuilder(SchemaTableName tableName, Optional<String> comment)
        {
            this.tableName = tableName;
            this.comment = comment;
        }

        public TableMetadataBuilder column(String columnName, Type type)
        {
            columns.add(new ColumnMetadata(columnName, type));
            return this;
        }

        public TableMetadataBuilder property(String name, Object value)
        {
            properties.put(name, value);
            return this;
        }

        public ConnectorTableMetadata build()
        {
            return new ConnectorTableMetadata(tableName, columns.build(), properties.build(), comment);
        }
    }
}
