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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.operator.aggregation.state.NetflixHistogramState;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.VarbinaryType;
import com.netflix.data.datastructures.NetflixHistogram;
import com.netflix.data.datastructures.NetflixHistogramException;
import com.netflix.data.datastructures.NetflixHistogramTypes;
import io.airlift.slice.Slices;

import static com.facebook.presto.operator.aggregation.NetflixBuildSketchBytesAggregations.NAME;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.TDIGEST_DEFAULT_COMPRESSION;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.YAHOO_QUANTILE_SKETCH_DEFAULT_K;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.addValue;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.combineStates;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.getHistogramType;

@AggregationFunction(NAME)
public class NetflixBuildSketchBytesAggregations
{
    private NetflixBuildSketchBytesAggregations() {}

    public static final String NAME = "nf_build_sketch_bytes";

    @InputFunction
    public static void input(@AggregationState NetflixHistogramState state, @SqlType(StandardTypes.DOUBLE) double value)
    {
        input(state, value, NetflixHistogramTypes.TDigest.getValue(), TDIGEST_DEFAULT_COMPRESSION);
    }

    @InputFunction
    public static void input(@AggregationState NetflixHistogramState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long histogramType)
    {
        NetflixHistogramTypes type = getHistogramType((int) histogramType);
        input(state, value, histogramType, type == NetflixHistogramTypes.TDigest ? TDIGEST_DEFAULT_COMPRESSION : YAHOO_QUANTILE_SKETCH_DEFAULT_K);
    }

    @InputFunction
    public static void input(@AggregationState NetflixHistogramState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long histogramType, @SqlType(StandardTypes.BIGINT) long k)
    {
        addValue(state, value, 1, histogramType, k);
    }

    @CombineFunction
    public static void combine(@AggregationState NetflixHistogramState state, NetflixHistogramState otherState)
            throws NetflixHistogramException
    {
        combineStates(state, otherState);
    }

    @OutputFunction("varbinary")
    public static void output(@AggregationState NetflixHistogramState state, BlockBuilder out)
    {
        NetflixHistogram digest = state.getDigest();
        if (digest == null) {
            out.appendNull();
        }
        else {
            VarbinaryType.VARBINARY.writeSlice(out, Slices.wrappedBuffer(digest.toBytes()));
        }
    }
}
