/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec.compression;

import java.io.InputStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import net.jpountz.lz4.LZ4BlockInputStream;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class Lz4FrameEncoderTest extends AbstractEncoderTest {

    @Mock
    private ChannelHandlerContext ctx;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(ctx.alloc()).thenReturn(PooledByteBufAllocator.DEFAULT);
    }

    @Override
    public void initChannel() {
        channel = new EmbeddedChannel(new Lz4FrameEncoder());
    }

    @Override
    protected ByteBuf decompress(ByteBuf compressed, int originalLength) throws Exception {
        InputStream is = new ByteBufInputStream(compressed);
        LZ4BlockInputStream lz4Is = new LZ4BlockInputStream(is);

        byte[] decompressed = new byte[originalLength];
        int remaining = originalLength;
        while (remaining > 0) {
            int read = lz4Is.read(decompressed, originalLength - remaining, remaining);
            if (read > 0) {
                remaining -= read;
            } else {
                break;
            }
        }
        assertEquals(-1, lz4Is.read());
        lz4Is.close();

        return Unpooled.wrappedBuffer(decompressed);
    }

    @Test
    public void testAllocateDirectBuffer() {
        Lz4FrameEncoder encoder = new Lz4FrameEncoder();
        final int blockSize = encoder.blockSize();
        testAllocateBuffer(blockSize - 13, true);
        testAllocateBuffer(blockSize * 5, true);
    }

    @Test
    public void testAllocateHeapBuffer() {
        Lz4FrameEncoder encoder = new Lz4FrameEncoder();
        final int blockSize = encoder.blockSize();
        testAllocateBuffer(blockSize - 13, false);
        testAllocateBuffer(blockSize * 5, false);
    }

    private void testAllocateBuffer(int bufSize, boolean isDirect) {
        // allocate the input buffer to an arbitrary size less than the blockSize
        ByteBuf in = PooledByteBufAllocator.DEFAULT.buffer(bufSize, bufSize);
        in.writerIndex(in.capacity());

        ByteBuf out = null;
        try {
            out = new Lz4FrameEncoder().allocateBuffer(ctx, in, isDirect);
            Assert.assertNotNull(out);
            Assert.assertTrue(out.writableBytes() > 0);
            Assert.assertEquals(isDirect, out.isDirect());
        } finally {
            in.release();
            if (out != null) {
                out.release();
            }
        }
    }

    @Test
    public void testFlush() {
        Lz4FrameEncoder encoder = new Lz4FrameEncoder();
        EmbeddedChannel channel = new EmbeddedChannel(encoder);
        int size = 27;
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(size, size);
        buf.writerIndex(size);
        Assert.assertEquals(0, encoder.currentBlockLength());
        channel.write(buf);
        Assert.assertTrue(channel.outboundMessages().isEmpty());
        Assert.assertEquals(size, encoder.currentBlockLength());
        channel.flush();
        Assert.assertTrue(channel.finish());
        Assert.assertTrue(channel.releaseOutbound());
        Assert.assertFalse(channel.releaseInbound());
    }
}
