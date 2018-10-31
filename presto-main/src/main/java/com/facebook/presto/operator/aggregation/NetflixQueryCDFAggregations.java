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

import com.facebook.presto.operator.aggregation.state.NetflixQueryCDFArrayState;
import com.facebook.presto.operator.aggregation.state.NetflixQueryCDFState;
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
import com.google.common.collect.ImmutableList;
import com.netflix.data.datastructures.NetflixHistogram;
import com.netflix.data.datastructures.NetflixHistogramException;
import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.operator.aggregation.NetflixErrorCode.NETFLIX_HISTOGRAM_IO_ERROR;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.addSketchString;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.combineStates;
import static com.facebook.presto.operator.aggregation.NetflixQueryCDFAggregations.NAME;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.util.Failures.checkCondition;

@AggregationFunction(NAME)
public class NetflixQueryCDFAggregations
{
    private NetflixQueryCDFAggregations() {}

    static final String NAME = "nf_query_cdf_from_sketch_string";

    @InputFunction
    public static void input(@AggregationState NetflixQueryCDFState state, @SqlType(StandardTypes.VARCHAR) Slice sketch, @SqlType(StandardTypes.DOUBLE) double cdf)
    {
        try {
            addSketchString(state, sketch);
            state.setCDF(cdf);
        }
        catch (NetflixHistogramException e) {
            throw new PrestoException(NETFLIX_HISTOGRAM_IO_ERROR, e);
        }
    }

    @InputFunction
    public static void input(@AggregationState NetflixQueryCDFArrayState state, @SqlType(StandardTypes.VARCHAR) Slice sketch, @SqlType("array(double)") Block cdfsArrayBlock)
    {
        try {
            addSketchString(state, sketch);
            initializeCDFSArray(state, cdfsArrayBlock);
        }
        catch (NetflixHistogramException e) {
            throw new PrestoException(NETFLIX_HISTOGRAM_IO_ERROR, e);
        }
    }

    @CombineFunction
    public static void combine(NetflixQueryCDFState state, NetflixQueryCDFState otherState)
            throws NetflixHistogramException
    {
        combineStates(state, otherState);
        state.setCDF(otherState.getCDF());
    }

    @CombineFunction
    public static void combine(NetflixQueryCDFArrayState state, NetflixQueryCDFArrayState otherState)
            throws NetflixHistogramException
    {
        combineStates(state, otherState);
        state.setCDFS(otherState.getCDFS());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState NetflixQueryCDFState state, BlockBuilder out)
    {
        NetflixHistogram digest = state.getDigest();
        double cdf = state.getCDF();
        if (digest == null || digest.getN() == 0) {
            out.appendNull();
        }
        else {
            double value = digest.cdf(cdf);
            DOUBLE.writeDouble(out, value);
        }
    }

    @OutputFunction("array(double)")
    public static void output(@AggregationState NetflixQueryCDFArrayState state, BlockBuilder out)
    {
        NetflixHistogram digest = state.getDigest();
        List<Double> cdfs = state.getCDFS();

        if (cdfs == null || digest == null) {
            out.appendNull();
            return;
        }

        BlockBuilder blockBuilder = out.beginBlockEntry();

        for (int i = 0; i < cdfs.size(); i++) {
            Double cdf = cdfs.get(i);
            DOUBLE.writeDouble(blockBuilder, digest.cdf(cdf));
        }

        out.closeEntry();
    }

    private static void initializeCDFSArray(@AggregationState NetflixQueryCDFArrayState state, Block cdfsArrayBlock)
    {
        if (state.getCDFS() == null) {
            ImmutableList.Builder<Double> cdfsListBuilder = ImmutableList.builder();

            for (int i = 0; i < cdfsArrayBlock.getPositionCount(); i++) {
                checkCondition(!cdfsArrayBlock.isNull(i), INVALID_FUNCTION_ARGUMENT, "cdf cannot be null");
                double cdf = DOUBLE.getDouble(cdfsArrayBlock, i);
                cdfsListBuilder.add(cdf);
            }
            state.setCDFS(cdfsListBuilder.build());
        }
    }
}
