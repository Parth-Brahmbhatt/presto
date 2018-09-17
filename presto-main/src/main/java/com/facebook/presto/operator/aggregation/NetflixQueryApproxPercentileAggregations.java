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
import com.netflix.data.datastructures.NetflixHistogramTypes;

import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.TDIGEST_DEFAULT_COMPRESSION;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.YAHOO_QUANTILE_SKETCH_DEFAULT_K;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.addValue;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.combineStates;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.getHistogramType;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.initializePercentilesArray;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.writePercentileValue;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.writePercentileValues;
import static com.facebook.presto.operator.aggregation.NetflixQueryApproxPercentileAggregations.NAME;

@AggregationFunction(NAME)
public class NetflixQueryApproxPercentileAggregations
{
    private NetflixQueryApproxPercentileAggregations() {}

    public static final String NAME = "nf_query_approx_percentile";

    @InputFunction
    public static void input(@AggregationState NetflixQueryApproxPercentileState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        weightedInput(state, value, 1, percentile, NetflixHistogramTypes.TDigest.getValue(), TDIGEST_DEFAULT_COMPRESSION);
    }

    @InputFunction
    public static void weightedInput(@AggregationState NetflixQueryApproxPercentileState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType(StandardTypes.DOUBLE) double percentile)
    {
        weightedInput(state, value, weight, percentile, NetflixHistogramTypes.TDigest.getValue(), TDIGEST_DEFAULT_COMPRESSION);
    }

    @InputFunction
    public static void input(@AggregationState NetflixQueryApproxPercentileState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double percentile, @SqlType(StandardTypes.BIGINT) long histogramType)
    {
        NetflixHistogramTypes type = getHistogramType((int) histogramType);
        weightedInput(state, value, 1, percentile, histogramType, type == NetflixHistogramTypes.TDigest ? TDIGEST_DEFAULT_COMPRESSION : YAHOO_QUANTILE_SKETCH_DEFAULT_K);
    }

    @InputFunction
    public static void weightedInput(@AggregationState NetflixQueryApproxPercentileState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType(StandardTypes.DOUBLE) double percentile, @SqlType(StandardTypes.BIGINT) long histogramType)
    {
        NetflixHistogramTypes type = getHistogramType((int) histogramType);
        weightedInput(state, value, weight, percentile, histogramType, type == NetflixHistogramTypes.TDigest ? TDIGEST_DEFAULT_COMPRESSION : YAHOO_QUANTILE_SKETCH_DEFAULT_K);
    }

    @InputFunction
    public static void input(@AggregationState NetflixQueryApproxPercentileState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.DOUBLE) double percentile, @SqlType(StandardTypes.BIGINT) long histogramType, @SqlType(StandardTypes.BIGINT) long k)
    {
        weightedInput(state, value, 1, percentile, histogramType, k);
    }

    @InputFunction
    public static void weightedInput(@AggregationState NetflixQueryApproxPercentileState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType(StandardTypes.DOUBLE) double percentile, @SqlType(StandardTypes.BIGINT) long histogramType, @SqlType(StandardTypes.BIGINT) long k)
    {
        addValue(state, value, weight, histogramType, k);
        // use last percentile
        state.setPercentile(percentile);
    }

    @InputFunction
    public static void input(@AggregationState NetflixQueryApproxPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        weightedInput(state, value, 1, percentilesArrayBlock, NetflixHistogramTypes.TDigest.getValue(), TDIGEST_DEFAULT_COMPRESSION);
    }

    @InputFunction
    public static void weightedInput(@AggregationState NetflixQueryApproxPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType("array(double)") Block percentilesArrayBlock)
    {
        weightedInput(state, value, weight, percentilesArrayBlock, NetflixHistogramTypes.TDigest.getValue(), TDIGEST_DEFAULT_COMPRESSION);
    }

    @InputFunction
    public static void input(@AggregationState NetflixQueryApproxPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType("array(double)") Block percentilesArrayBlock, @SqlType(StandardTypes.BIGINT) long histogramType)
    {
        NetflixHistogramTypes type = getHistogramType((int) histogramType);
        weightedInput(state, value, 1, percentilesArrayBlock, histogramType, type == NetflixHistogramTypes.TDigest ? TDIGEST_DEFAULT_COMPRESSION : YAHOO_QUANTILE_SKETCH_DEFAULT_K);
    }

    @InputFunction
    public static void weightedInput(@AggregationState NetflixQueryApproxPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType("array(double)") Block percentilesArrayBlock, @SqlType(StandardTypes.BIGINT) long histogramType)
    {
        NetflixHistogramTypes type = getHistogramType((int) histogramType);
        weightedInput(state, value, weight, percentilesArrayBlock, histogramType, type == NetflixHistogramTypes.TDigest ? TDIGEST_DEFAULT_COMPRESSION : YAHOO_QUANTILE_SKETCH_DEFAULT_K);
    }

    @InputFunction
    public static void input(@AggregationState NetflixQueryApproxPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType("array(double)") Block percentilesArrayBlock, @SqlType(StandardTypes.BIGINT) long histogramType, @SqlType(StandardTypes.BIGINT) long k)
    {
        weightedInput(state, value, 1, percentilesArrayBlock, histogramType, k);
    }

    @InputFunction
    public static void weightedInput(@AggregationState NetflixQueryApproxPercentileArrayState state, @SqlType(StandardTypes.DOUBLE) double value, @SqlType(StandardTypes.BIGINT) long weight, @SqlType("array(double)") Block percentilesArrayBlock, @SqlType(StandardTypes.BIGINT) long histogramType, @SqlType(StandardTypes.BIGINT) long k)
    {
        addValue(state, value, weight, histogramType, k);
        initializePercentilesArray(state, percentilesArrayBlock);
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
