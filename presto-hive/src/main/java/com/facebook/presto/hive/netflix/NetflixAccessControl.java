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
package com.facebook.presto.hive.netflix;

import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveConnectorId;
import com.facebook.presto.hive.metastore.thrift.HiveMetastore;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorAccessControl;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.AccessDeniedException;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.Privilege;
import com.google.common.base.Splitter;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hive.HiveSessionProperties.AWS_IAM_ROLE;
import static com.facebook.presto.spi.security.AccessDeniedException.denyDropTable;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NetflixAccessControl
        implements ConnectorAccessControl
{
    private static final Splitter SPLITTER = Splitter.on('=').trimResults().omitEmptyStrings();

    private final HiveMetastore metastore;
    private HiveConnectorId connectorId;
    private List<String> s3RoleMappings;

    @Inject
    public NetflixAccessControl(HiveMetastore metastore, HiveConnectorId connectorId, HiveClientConfig hiveClientConfig)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.connectorId = connectorId;
        requireNonNull(hiveClientConfig, "hiveClientConfig is null");
        this.s3RoleMappings = requireNonNull(hiveClientConfig.getS3RoleMappings(), "s3RoleMappings is null");
    }

    @Override
    public void checkCanCreateSchema(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName)
    {
    }

    @Override
    public void checkCanDropSchema(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName)
    {
    }

    @Override
    public void checkCanRenameSchema(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName, String newSchemaName)
    {
    }

    @Override
    public void checkCanShowSchemas(ConnectorTransactionHandle transactionHandle, Identity identity)
    {
    }

    @Override
    public Set<String> filterSchemas(ConnectorTransactionHandle transactionHandle, Identity identity, Set<String> schemaNames)
    {
        return schemaNames;
    }

    @Override
    public void checkCanShowTablesMetadata(ConnectorTransactionHandle transactionHandle, Identity identity, String schemaName)
    {
    }

    @Override
    public Set<SchemaTableName> filterTables(ConnectorTransactionHandle transactionHandle, Identity identity, Set<SchemaTableName> tableNames)
    {
        return tableNames;
    }

    @Override
    public void checkCanCreateTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanDropTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        Optional<Table> target = metastore.getTable(tableName.getSchemaName(), tableName.getTableName());

        if (!target.isPresent()) {
            denyDropTable(tableName.toString(), "Table not found");
        }
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanRenameTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName, SchemaTableName newTableName)
    {
    }

    @Override
    public void checkCanAddColumn(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanDropColumn(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanRenameColumn(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanSelectFromColumns(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName, Set<String> colunmNames)
    {
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanInsertIntoTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanDeleteFromTable(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName tableName)
    {
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanCreateView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
        checkAccess(identity, viewName);
    }

    @Override
    public void checkCanDropView(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName)
    {
        checkAccess(identity, viewName);
    }

    @Override
    public void checkCanCreateViewWithSelectFromColumns(ConnectorTransactionHandle transaction, Identity identity, SchemaTableName viewName, Set<String> columnNames)
    {
        checkAccess(identity, viewName);
    }

    @Override
    public void checkCanSetCatalogSessionProperty(ConnectorTransactionHandle transactionHandle, Identity identity, String propertyName)
    {
    }

    @Override
    public void checkCanGrantTablePrivilege(ConnectorTransactionHandle transactionHandle, Identity identity, Privilege privilege, SchemaTableName tableName, String grantee, boolean withGrantOption)
    {
        checkAccess(identity, tableName);
    }

    @Override
    public void checkCanRevokeTablePrivilege(ConnectorTransactionHandle transactionHandle, Identity identity, Privilege privilege, SchemaTableName tableName, String grantee, boolean withGrantOption)
    {
        checkAccess(identity, tableName);
    }

    private void checkAccess(Identity identity, SchemaTableName tableName)
    {
        for (String roleMapping : this.s3RoleMappings) {
            List<String> splitted = SPLITTER.splitToList(roleMapping);
            checkArgument(splitted.size() == 2, "Splitted s3 role mapping should have two elements (e.g., schema=role)");
            String schema = splitted.get(0);
            String roleName = splitted.get(1);
            // the user is accessing a schema that has a configured role mapping
            if (Objects.equals(tableName.getSchemaName(), schema)) {
                Map<String, Map<String, String>> sessionPropertiesByCatalog = identity.getSessionPropertiesByCatalog();

                if (sessionPropertiesByCatalog == null || sessionPropertiesByCatalog.get(this.connectorId.toString()) == null) {
                    denyAccess(schema);
                }

                Optional<Map.Entry<String, String>> roleProperty = sessionPropertiesByCatalog
                        .get(this.connectorId.toString())
                        .entrySet()
                        .stream()
                        .filter(entry -> entry.getKey().equalsIgnoreCase(AWS_IAM_ROLE + "_" + schema)).findAny();

                if (roleProperty.isPresent() == false) {
                    roleProperty = sessionPropertiesByCatalog
                            .get(this.connectorId.toString())
                            .entrySet()
                            .stream()
                            .filter(entry -> entry.getKey().equalsIgnoreCase(AWS_IAM_ROLE)).findAny();
                }

                if (!roleProperty.isPresent()) {
                    denyAccess(schema);
                }

                String sessionRoleName = roleProperty.get().getValue();
                if (!Objects.equals(roleName, sessionRoleName)) {
                    denyAccess(schema);
                }
            }
        }
    }

    private void denyAccess(String schema)
    {
        throw new AccessDeniedException(format("To access this table you should specify the role arn with the %s_%s session property " +
                        "on the %s catalog: \"set session %s.%s_%s=<role_arn>\"",
                AWS_IAM_ROLE, schema, this.connectorId, this.connectorId, AWS_IAM_ROLE, schema));
    }
}
