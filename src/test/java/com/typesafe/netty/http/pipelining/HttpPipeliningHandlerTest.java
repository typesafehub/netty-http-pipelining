package com.typesafe.netty.http.pipelining;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.HashedWheelTimer;
import static io.netty.buffer.Unpooled.unreleasableBuffer;
import static io.netty.buffer.Unpooled.copiedBuffer;
import static io.netty.handler.codec.http.HttpVersion.*;
import static io.netty.handler.codec.http.HttpHeaderNames.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

/**
 * Replacement of https://github.com/typesafehub/netty-http-pipelining/blob/master/src/test/java/com/typesafe/netty/http/pipelining/HttpPipeliningHandlerTest.java
 */
public class HttpPipeliningHandlerTest {

    private static final long RESPONSE_TIMEOUT = 20000L;
    private static final long CONNECTION_TIMEOUT = 10000L;
    public final static String CONTENT_TYPE_TEXT = "text/plain; charset=UTF-8";
    private static final InetSocketAddress HOST_ADDR = new InetSocketAddress("127.0.0.1", 9080);
    public static final String PATH1 = "/1";
    public static final String PATH2 = "/2";
    public static final String PATH3 = "/3";
    public static final String SOME_RESPONSE_TEXT = "some text for ";

    private Bootstrap clientBootstrap;
    private ServerBootstrap serverBootstrap;
    private EventLoopGroup clientWorkerGroup;
    private EventLoopGroup serverBossGroup;
    private EventLoopGroup serverWorkerGroup;

    private CountDownLatch responsesIn;
    private final List<String> responses = new ArrayList<String>(2);

    @Before
    public void setUp() {
        clientWorkerGroup = new NioEventLoopGroup();
        clientBootstrap = new Bootstrap();
        clientBootstrap.group(clientWorkerGroup);
        clientBootstrap.channel(NioSocketChannel.class);
        clientBootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        clientBootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("httpClientCodec", new HttpClientCodec());
                ch.pipeline().addLast("clientHandler", new ClientHandler());
            }

        });
        serverBossGroup = new NioEventLoopGroup(1);
        serverWorkerGroup = new NioEventLoopGroup();
        serverBootstrap = new ServerBootstrap();
        serverBootstrap.option(ChannelOption.SO_BACKLOG, 1024);
        serverBootstrap.group(serverBossGroup, serverWorkerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast("serverCodec", new HttpServerCodec());
                        //ch.pipeline().addLast("aggregator", new HttpObjectAggregator(65536));
                        //ch.pipeline().addLast("chunkedWriter", new ChunkedWriteHandler());
                        ch.pipeline().addLast("pipeliningHandler", new HttpPipeliningHandler());
                        ch.pipeline().addLast("serverHandler", new ServerHandler());
                    }
                });
        serverBootstrap.bind(HOST_ADDR);
    }

    @After
    public void shutDown() {
        serverBossGroup.shutdownGracefully();
        serverWorkerGroup.shutdownGracefully();
        clientWorkerGroup.shutdownGracefully();
    }

    @Test
    public void shouldReturnMessagesInOrder() throws InterruptedException {
        responsesIn = new CountDownLatch(1);
        responses.clear();

        final ChannelFuture connectionFuture = clientBootstrap.connect(HOST_ADDR);

        assertTrue(connectionFuture.await(CONNECTION_TIMEOUT));
        final Channel clientChannel = connectionFuture.channel();

        //ByteBuf content1 = clientChannel.alloc().buffer();
        //final ByteBuf REQUEST_BYTES_PATH1 = unreleasableBuffer(copiedBuffer(SOME_RESPONSE_TEXT + PATH1, CharsetUtil.UTF_8));
        //content1.writeBytes(REQUEST_BYTES_PATH1.duplicate());
        final HttpRequest request1 = new DefaultHttpRequest(
                HTTP_1_1, HttpMethod.GET, PATH1);
        request1.headers().set(HOST, HOST_ADDR.toString());
        //request1.headers().setInt(CONTENT_LENGTH, content1.readableBytes());

        //ByteBuf content2 = clientChannel.alloc().buffer();
        //final ByteBuf REQUEST_BYTES_PATH2 = unreleasableBuffer(copiedBuffer(SOME_RESPONSE_TEXT + PATH2, CharsetUtil.UTF_8));
        //content2.writeBytes(REQUEST_BYTES_PATH2.duplicate());
        final HttpRequest request2 = new DefaultHttpRequest(
                HTTP_1_1, HttpMethod.GET, PATH2);
        request2.headers().set(HOST, HOST_ADDR.toString());
        //request2.headers().setInt(CONTENT_LENGTH, content2.readableBytes());

        final HttpRequest request3 = new DefaultHttpRequest(
                HTTP_1_1, HttpMethod.GET, PATH3);
        request3.headers().set(HOST, HOST_ADDR.toString());

        ChannelPromise promise1 = clientChannel.newPromise();
        clientChannel.writeAndFlush(request1, promise1);

        ChannelPromise promise2 = clientChannel.newPromise();
        clientChannel.writeAndFlush(request2, promise2);

        ChannelPromise promise3 = clientChannel.newPromise();
        clientChannel.writeAndFlush(request3, promise3);

        responsesIn.await(RESPONSE_TIMEOUT, MILLISECONDS);

        assertTrue(responses.contains(SOME_RESPONSE_TEXT + PATH1));
        assertTrue(responses.contains(SOME_RESPONSE_TEXT + PATH2));
        assertTrue(responses.contains(SOME_RESPONSE_TEXT + PATH3));
    }

    public class ClientHandler extends ChannelHandlerAdapter {
        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
            if (msg instanceof HttpContent) {
                final HttpContent response = (HttpContent) msg;
                final String content = response.content().toString(CharsetUtil.UTF_8);
                responses.add(content);
                if (content.equals(SOME_RESPONSE_TEXT + PATH3)) {
                    responsesIn.countDown();
                }
            }
        }
    }

    public class ServerHandler extends ChannelHandlerAdapter {
        private final AtomicBoolean sendFinalChunk = new AtomicBoolean(false);
        private final HashedWheelTimer timer = new HashedWheelTimer();
        private final byte[] BYTES = new byte[1024 * 64];
        private final byte[] EMPTY_BYTES = new byte[0];

        @Override
        public void channelRead(final ChannelHandlerContext ctx,
                                    final Object msg) throws InterruptedException {
            if (msg instanceof OrderedInboundEvent) {
                OrderedInboundEvent orderedInboundEvent = (OrderedInboundEvent) msg;
                final HttpRequest httpRequest = (HttpRequest) orderedInboundEvent.getMessage();
                final String uri = httpRequest.uri();
                final ByteBuf RESPONSE_BYTES = unreleasableBuffer(copiedBuffer(SOME_RESPONSE_TEXT + uri, CharsetUtil.UTF_8));

                boolean keepAlive = HttpHeaderUtil.isKeepAlive(httpRequest);
                ByteBuf content = ctx.alloc().buffer();
                content.writeBytes(RESPONSE_BYTES.duplicate());

                FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, content);
                response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
                response.headers().setInt(CONTENT_LENGTH, response.content().readableBytes());

                if (!keepAlive) {
                    ctx.writeAndFlush(new OrderedOutboundEvent(0, orderedInboundEvent, false, response)).addListener(ChannelFutureListener.CLOSE);
                } else {
                    response.headers().set(CONNECTION, HttpHeaderValues.KEEP_ALIVE);
                    ctx.writeAndFlush(new OrderedOutboundEvent(0, orderedInboundEvent, false, response));
                }
            }
        }

        /*private class ChunkWriter implements TimerTask {
            private final ChannelHandlerContext ctx;
            private final String uri;
            private final OrderedInboundEvent oie;
            private final int subSequence;

            public ChunkWriter(final ChannelHandlerContext ctx, final String uri,
                               final OrderedInboundEvent oie, final int subSequence) {
                this.ctx = ctx;
                this.uri = uri;
                this.oie = oie;
                this.subSequence = subSequence;
            }

            @Override
            public void run(final Timeout timeout) {
                try {
                    if (sendFinalChunk.get() && subSequence > 1) {
                        ChunkedInput httpChunk = new HttpChunkedInput(new ChunkedStream(new ByteArrayInputStream(EMPTY_BYTES)));
                        ctx.writeAndFlush(new OrderedOutboundEvent(subSequence, oie, true, httpChunk));
                    } else {
                        ChunkedInput httpChunk = new HttpChunkedInput(new ChunkedFile(TMP));
                        ctx.writeAndFlush(new OrderedOutboundEvent(subSequence, oie, false, httpChunk));
                        timer.newTimeout(new ChunkWriter(ctx, uri, oie, subSequence + 1), 0, MILLISECONDS);
                        if (uri.equals(HttpPipeliningHandlerTest.PATH2)) {
                            sendFinalChunk.set(true);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }*/
    }
}

