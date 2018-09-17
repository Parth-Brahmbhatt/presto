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

import com.facebook.presto.operator.aggregation.state.NetflixQueryApproxPercentileArrayState;
import com.facebook.presto.operator.aggregation.state.NetflixQueryApproxPercentileState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AggregationFunction;
import com.facebook.presto.spi.function.AggregationState;
import com.facebook.presto.spi.function.CombineFunction;
import com.facebook.presto.spi.function.InputFunction;
import com.facebook.presto.spi.function.OutputFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import com.netflix.data.datastructures.NetflixHistogram;
import com.netflix.data.datastructures.NetflixHistogramException;
import io.airlift.slice.Slice;

import static com.facebook.presto.operator.aggregation.NetflixErrorCode.NETFLIX_HISTOGRAM_IO_ERROR;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.addSketchBytes;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.combineStates;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.initializePercentilesArray;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.writePercentileValue;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.writePercentileValues;
import static com.facebook.presto.operator.aggregation.NetflixQueryApproxPercentileAggregations.NAME;

@AggregationFunction(NAME)
public class NetflixQueryApproxPercentileSketchBytesAggregations
{
    private NetflixQueryApproxPercentileSketchBytesAggregations() {}

    @InputFunction
    public static void input(@AggregationState NetflixQueryApproxPercentileState state, @SqlType(StandardTypes.VARBINARY) Slice sketch, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        try {
            addSketchBytes(state, sketch);
            state.setPercentile(percentile);
        }
        catch (NetflixHistogramException e) {
            throw new PrestoException(NETFLIX_HISTOGRAM_IO_ERROR, e);
        }
    }

    @InputFunction
    public static void input(@AggregationState NetflixQueryApproxPercentileArrayState state, @SqlType(StandardTypes.VARBINARY) Slice sketch, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        try {
            addSketchBytes(state, sketch);
            initializePercentilesArray(state, percentilesArrayBlock);
        }
        catch (NetflixHistogramException e) {
            throw new PrestoException(NETFLIX_HISTOGRAM_IO_ERROR, e);
        }
    }

    @CombineFunction
    public static void combine(@AggregationState NetflixQueryApproxPercentileState state, NetflixQueryApproxPercentileState otherState)
            throws NetflixHistogramException
    {
        combineStates(state, otherState);
        state.setPercentile(otherState.getPercentile());
    }

    @CombineFunction
    public static void combine(@AggregationState NetflixQueryApproxPercentileArrayState state, NetflixQueryApproxPercentileArrayState otherState)
            throws NetflixHistogramException
    {
        combineStates(state, otherState);
        state.setPercentiles(otherState.getPercentiles());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState NetflixQueryApproxPercentileState state, BlockBuilder out)
    {
        NetflixHistogram digest = state.getDigest();
        double percentile = state.getPercentile();
        writePercentileValue(out, digest, percentile);
    }

    @OutputFunction("array(double)")
    public static void output(@AggregationState NetflixQueryApproxPercentileArrayState state, BlockBuilder out)
    {
        writePercentileValues(state, out);
    }
}
