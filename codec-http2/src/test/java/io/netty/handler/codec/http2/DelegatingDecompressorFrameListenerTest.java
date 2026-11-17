/*
 * Copyright 2025 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.netty.handler.codec.http2;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.compression.Brotli;
import io.netty.handler.codec.compression.BrotliEncoder;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DelegatingDecompressorFrameListenerTest {

    static String[] encodings() {
        List<String> encodings = new ArrayList<String>();
        encodings.add("gzip");
        encodings.add("deflate");
        if (Brotli.isAvailable()) {
            encodings.add("br");
        }
        return encodings.toArray(new String[0]);
    }

    private static EmbeddedChannel newCompressChannel(String encoding) {
        if ("gzip".equals(encoding)) {
            return new EmbeddedChannel(ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP));
        }
        if ("deflate".equals(encoding)) {
            return new EmbeddedChannel(ZlibCodecFactory.newZlibEncoder(ZlibWrapper.ZLIB));
        }
        return new EmbeddedChannel(new BrotliEncoder());
    }

    @ParameterizedTest
    @MethodSource("encodings")
    public void testZipBomb(String encoding) throws Exception {
        int chunkSize = 1024 * 1024;
        int numberOfChunks = 256;
        int memoryLimit = chunkSize * 128;

        EmbeddedChannel compressChannel = newCompressChannel(encoding);
        ByteBuf compressed = Unpooled.buffer();
        for (int i = 0; i <= numberOfChunks; i++) {
            if (i < numberOfChunks) {
                ByteBuf in = compressChannel.alloc().buffer(chunkSize);
                in.writeZero(chunkSize);
                compressChannel.writeOutbound(in);
            } else {
                compressChannel.close();
            }
            while (true) {
                ByteBuf buf = compressChannel.readOutbound();
                if (buf == null) {
                    break;
                }
                compressed.writeBytes(buf);
                buf.release();
            }
        }

        PooledByteBufAllocator allocator = new PooledByteBufAllocator(false);
        EmbeddedChannel channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        channel.config().setAllocator(allocator);
        ChannelHandlerContext ctx = channel.pipeline().firstContext();

        Http2Connection connection = new DefaultHttp2Connection(true);
        DefaultHttp2LocalFlowController flowController = new DefaultHttp2LocalFlowController(connection);
        flowController.channelHandlerContext(ctx);
        connection.local().flowController(flowController);
        int streamId = 3;
        connection.remote().createStream(streamId, false);

        ZipBombFrameListener listener = new ZipBombFrameListener(memoryLimit);
        DelegatingDecompressorFrameListener decompressor =
                new DelegatingDecompressorFrameListener(connection, listener);
        Http2Headers headers = new DefaultHttp2Headers().set(HttpHeaderNames.CONTENT_ENCODING, encoding);
        try {
            decompressor.onHeadersRead(ctx, streamId, headers, 0, false);
            decompressor.onDataRead(ctx, streamId, compressed, 0, true);
        } finally {
            compressed.release();
            channel.finishAndReleaseAll();
        }

        assertEquals((long) chunkSize * numberOfChunks, listener.total);
        assertTrue(listener.endOfStream);
    }

    private static final class ZipBombFrameListener extends Http2FrameAdapter {
        final int memoryLimit;
        long total;
        boolean endOfStream;

        ZipBombFrameListener(int memoryLimit) {
            this.memoryLimit = memoryLimit;
        }

        @Override
        public int onDataRead(ChannelHandlerContext ctx, int streamId, ByteBuf data, int padding,
                              boolean endOfStream) {
            PooledByteBufAllocator allocator = (PooledByteBufAllocator) ctx.alloc();
            assertTrue(allocator.metric().usedHeapMemory() < memoryLimit);
            assertTrue(allocator.metric().usedDirectMemory() < memoryLimit);

            total += data.readableBytes();
            if (endOfStream) {
                this.endOfStream = true;
            }
            // The bytes are not returned to the flow controller as this test does not account for received bytes.
            return 0;
        }
    }
}
