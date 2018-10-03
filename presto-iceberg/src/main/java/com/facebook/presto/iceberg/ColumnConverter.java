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

import com.facebook.presto.iceberg.type.TypeConveter;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.netflix.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;

import static com.netflix.iceberg.types.Types.NestedField.required;

public class ColumnConverter
{
    private ColumnConverter()
    {
    }

    public static List<Types.NestedField> toIceberg(List<ColumnMetadata> columns)
    {
        List<Types.NestedField> icebergColumns = new ArrayList<>();
        for (ColumnMetadata column : columns) {
            final String name = column.getName();
            final Type type = column.getType();
            final com.netflix.iceberg.types.Type icebergType = TypeConveter.convert(type);
            icebergColumns.add(required(icebergColumns.size(), name, icebergType));
        }
        return icebergColumns;
    }
}
