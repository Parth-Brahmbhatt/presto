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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static com.facebook.presto.operator.aggregation.NetflixHistogramUtils.deserializeHistogram;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public class NetflixQueryCDFStateSerializer
        implements AccumulatorStateSerializer<NetflixQueryCDFState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(NetflixQueryCDFState state, BlockBuilder out)
    {
        if (state.getDigest() == null) {
            out.appendNull();
        }
        else {
            Slice serialized = Slices.wrappedBuffer(state.getDigest().toBytes());
            SliceOutput output = Slices.allocate(SIZE_OF_DOUBLE + SIZE_OF_INT + serialized.length()).getOutput();
            output.appendDouble(state.getCDF());
            output.appendInt(serialized.length());
            output.appendBytes(serialized);
            VARBINARY.writeSlice(out, output.slice());
        }
    }

    @Override
    public void deserialize(Block block, int index, NetflixQueryCDFState state)
    {
        SliceInput input = VARBINARY.getSlice(block, index).getInput();

        // read percentile
        // read digest
        state.setCDF(input.readDouble());
        int length = input.readInt();
        deserializeHistogram(state, input, length);
    }
}
