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

import com.netflix.iceberg.FileFormat;
import com.netflix.iceberg.PartitionField;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.hive.HiveTables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.util.ConfigurationUtils.getInitialConfiguration;
import static com.netflix.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static com.netflix.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

class IcebergUtil
{
    public static final String ICEBERG_PROPERTY_NAME = "table_type";
    public static final String ICEBERG_PROPERTY_VALUE = "iceberg";
    public static final String HIVE_WAREHOUSE_DIR = "metastore.warehouse.dir";
    private static final String PATH_SEPERATOR = "/";
    public static final String DATA_DIR_NAME = "data";

    private IcebergUtil() {}

    public static final boolean isIcebergTable(com.facebook.presto.hive.metastore.Table table)
    {
        final Map<String, String> parameters = table.getParameters();
        return parameters != null && !parameters.isEmpty() && ICEBERG_PROPERTY_VALUE.equalsIgnoreCase(parameters.get(ICEBERG_PROPERTY_NAME));
    }

    public static Table getIcebergTable(String database, String tableName, Configuration configuration)
    {
        // Reduce the load calls as they are expensive so may be we keep the table info
        return getHiveTables(configuration).load(database, tableName);
    }

    public static HiveTables getHiveTables(Configuration configuration)
    {
        return new HiveTables(configuration);
    }

    public static final List<PartitionField> getIdentityPartitions(PartitionSpec partitionSpec)
    {
        //TODO We are only treating identity column as partition columns as we do not want all other columns to be projectable or filterable.
        // Identity class is not public so no way to really identify if a transformation is identity transformation or not other than checking toString as of now.
        // Need to make changes to iceberg so we can identify transform in a better way.
        return partitionSpec.fields().stream().filter(partitionField -> partitionField.transform().toString().equals("identity")).collect(Collectors.toList());
    }

    public static final String getDataPath(String icebergLocation)
    {
        return icebergLocation.endsWith(PATH_SEPERATOR) ? icebergLocation + DATA_DIR_NAME : icebergLocation + PATH_SEPERATOR + DATA_DIR_NAME;
    }

    public static final String getTablePath(String schemaName, String tableName)
    {
        return new Path(getInitialConfiguration().get(HIVE_WAREHOUSE_DIR), String.format("%s.db", schemaName), tableName).toString();
    }

    public static final FileFormat getFileFormat(Table table)
    {
        return FileFormat.valueOf(table.properties()
                .getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT)
                .toUpperCase(Locale.ENGLISH));
    }
}
