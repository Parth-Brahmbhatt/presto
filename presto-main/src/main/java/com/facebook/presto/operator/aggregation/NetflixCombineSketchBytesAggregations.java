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
import com.facebook.presto.spi.PrestoException;
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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.operator.aggregation.NetflixCombineSketchBytesAggregations.NAME;
import static com.facebook.presto.operator.aggregation.NetflixErrorCode.NETFLIX_HISTOGRAM_IO_ERROR;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.addSketchBytes;
import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.combineStates;

@AggregationFunction(NAME)
public class NetflixCombineSketchBytesAggregations
{
    private NetflixCombineSketchBytesAggregations() {}

    public static final String NAME = "nf_combine_sketches";

    @InputFunction
    public static void input(@AggregationState NetflixHistogramState state, @SqlType(StandardTypes.VARBINARY) Slice sketch)
    {
        try {
            addSketchBytes(state, sketch);
        }
        catch (NetflixHistogramException e) {
            throw new PrestoException(NETFLIX_HISTOGRAM_IO_ERROR, e);
        }
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
