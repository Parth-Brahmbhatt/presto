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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.thrift.BridgingHiveMetastore;
import com.facebook.presto.hive.metastore.thrift.InMemoryHiveMetastore;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.tpch.TpchTable;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Map;

import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveQueryRunner.TPCH_SCHEMA;
import static com.facebook.presto.hive.HiveQueryRunner.createSession;
import static com.facebook.presto.hive.security.SqlStandardAccessControl.ADMIN_ROLE_NAME;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.tpch.TpchTable.ORDERS;

public class TestHiveView
        extends AbstractTestQueryFramework
{
    private static final TestQueryRunnerUtil util = new TestQueryRunnerUtil();

    public TestHiveView()
    {
        super(() -> util.createQueryRunner(ORDERS));
    }

    @Test
    public void testSelectOnView()
    {
        util.addView("test_hive_view", "user");
        assertQuery("SELECT * from test_hive_view", "SELECT * FROM orders");
        assertUpdate("DROP TABLE test_hive_view");
    }

    @Test(expectedExceptions = RuntimeException.class,
            expectedExceptionsMessageRegExp = "Access Denied: Cannot select from view tpch.test_hive_view1")
    public void testSelectOnViewWithoutPrivilege() throws Exception
    {
        util.addView("test_hive_view1", "user1");
        computeActual("SELECT * from test_hive_view1");
    }

    private static class TestQueryRunnerUtil
    {
        InMemoryHiveMetastore metastore;
        private static final DateTimeZone TIME_ZONE = DateTimeZone.forID("Asia/Kathmandu");

        public TestQueryRunnerUtil()
        {
        }

        public DistributedQueryRunner createQueryRunner(TpchTable<?>... tables)
                throws Exception
        {
            DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession()).setNodeCount(4).build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toFile();
                metastore = new InMemoryHiveMetastore(baseDir);
                metastore.setUserRoles(createSession().getUser(), ImmutableSet.of(ADMIN_ROLE_NAME));
                metastore.createDatabase(createDatabaseMetastoreObject(baseDir, TPCH_SCHEMA));
                queryRunner.installPlugin(new HivePlugin(HIVE_CATALOG, new BridgingHiveMetastore(metastore)));

                metastore.setUserRoles(createSession().getUser(), ImmutableSet.of("admin"));

                Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                        .putAll(ImmutableMap.of())
                        .put("hive.time-zone", TIME_ZONE.getID())
                        .put("hive.security", "sql-standard")
                        .build();
                queryRunner.createCatalog(HIVE_CATALOG, HIVE_CATALOG, hiveProperties);

                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), ImmutableList.copyOf(tables));
            }
            catch (Exception e) {
                queryRunner.close();
                throw e;
            }
            return queryRunner;
        }

        private static Database createDatabaseMetastoreObject(File baseDir, String name)
        {
            Database database = new Database(name, null, new File(baseDir, name).toURI().toString(), null);
            database.setOwnerName("public");
            database.setOwnerType(PrincipalType.ROLE);
            return database;
        }

        public void addView(String viewName, String owner)
        {
            String sql = "select * from orders";
            org.apache.hadoop.hive.metastore.api.Table table =
                    new org.apache.hadoop.hive.ql.metadata.Table(TPCH_SCHEMA, viewName).getTTable();
            table.setOwner(owner);
            table.setTableType(TableType.VIRTUAL_VIEW.name());
            table.setParameters(ImmutableMap.of());
            table.setPrivileges(buildInitialPrivilegeSet(owner));
            table.setViewOriginalText(sql);
            table.setViewExpandedText(sql);
            metastore.createTable(table);
        }

        private static PrincipalPrivilegeSet buildInitialPrivilegeSet(String tableOwner)
        {
            return new PrincipalPrivilegeSet(ImmutableMap.of(tableOwner, ImmutableList.of(
                    new PrivilegeGrantInfo("SELECT", 0, tableOwner, PrincipalType.USER, true),
                    new PrivilegeGrantInfo("INSERT", 0, tableOwner, PrincipalType.USER, true),
                    new PrivilegeGrantInfo("UPDATE", 0, tableOwner, PrincipalType.USER, true),
                    new PrivilegeGrantInfo("DELETE", 0, tableOwner, PrincipalType.USER, true))),
                    ImmutableMap.of(), ImmutableMap.of());
        }
    }
}
