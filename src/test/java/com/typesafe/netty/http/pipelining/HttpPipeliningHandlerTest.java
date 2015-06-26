package com.typesafe.netty.http.pipelining;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.Promise;
import org.junit.*;

import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class HttpPipeliningHandlerTest {

    private static NioEventLoopGroup group;
    private static Channel serverBindChannel;
    private static ServerHandler serverHandler;

    @BeforeClass
    public static void startServer() throws Exception {
        group = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap()
                .group(group)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.AUTO_READ, false)
                .localAddress("127.0.0.1", 0)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(
                                new HttpRequestDecoder(),
                                new HttpResponseEncoder(),
                                new HttpPipeliningHandler(2, 4),
                                serverHandler
                        );
                    }
                });

        serverBindChannel = await(serverBootstrap.bind()).channel();
    }

    private Channel clientChannel;
    private Channel serverChannel;
    private BlockingQueue<Object> serverMessagesReceived;
    private BlockingQueue<Object> clientMessagesReceived;

    @Before
    public void setUpConnection() throws Exception {
        serverMessagesReceived = new LinkedBlockingQueue<>();
        clientMessagesReceived = new LinkedBlockingQueue<>();
        final Promise<Channel> serverChannelPromise = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);

        serverHandler = new ServerHandler(serverMessagesReceived, serverChannelPromise);

        Bootstrap clientBootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(
                                new HttpClientCodec(),
                                new HttpObjectAggregator(1048576),
                                new ClientHandler(clientMessagesReceived)
                        );
                    }
                });

        SocketAddress bindAddress = serverBindChannel.localAddress();

        clientChannel = await(clientBootstrap.connect(bindAddress)).channel();
        serverChannel = await(serverChannelPromise).get();
    }

    @After
    public void closeConnection() throws Exception {
        await(clientChannel.close());
    }

    @AfterClass
    public static void shutDownServer() throws Exception {
        await(serverBindChannel.close());
        group.shutdownGracefully();
    }

    @Test
    public void noPipelining() throws Exception {
        // Request with simple response
        clientMakeRequest("/");
        SequencedHttpRequest request1 = serverReceiveRequest();
        serverWrite(new SequencedOutboundMessage(request1.getSequence(), createResponse("")), true);
        clientReceiveResponse();

        // Request with chunked response
        clientMakeRequest("/");
        SequencedHttpRequest request2 = serverReceiveRequest();
        serverSendChunkedResponse(request2, true, "hello", " ", "world");
        assertEquals("hello world", clientReceiveResponseBody());

        // Another request with chunked response, but don't wait for back pressure
        clientMakeRequest("/");
        SequencedHttpRequest request3 = serverReceiveRequest();
        serverSendChunkedResponse(request3, false, "foo", " ", "bar");
        serverChannel.flush();
        assertEquals("foo bar", clientReceiveResponseBody());

        // And another request with simple response
        clientMakeRequest("/");
        SequencedHttpRequest request4 = serverReceiveRequest();
        serverWrite(new SequencedOutboundMessage(request4.getSequence(), createResponse("simple")), true);
        assertEquals("simple", clientReceiveResponseBody());
    }

    @Test
    public void withPipelining() throws Exception {
        // Request with simple response
        clientMakeRequest("/");
        SequencedHttpRequest request1 = serverReceiveRequest();

        // Request with chunked response
        clientMakeRequest("/");
        SequencedHttpRequest request2 = serverReceiveRequest();

        // Another request with chunked response
        clientMakeRequest("/");
        SequencedHttpRequest request3 = serverReceiveRequest();

        // Another request with simple response
        clientMakeRequest("/");
        SequencedHttpRequest request4 = serverReceiveRequest();

        // Write the responses to request 2 and 4
        serverSendChunkedResponse(request2, false, "this", " is", " request", " 2");
        serverSendChunkedResponse(request4, false, "this", " is", " request", " 4");

        // Write the first response
        serverSendChunkedResponse(request1, true, "this", " is", " request", " 1");

        assertEquals("this is request 1", clientReceiveResponseBody());
        assertEquals("this is request 2", clientReceiveResponseBody());

        // Write the response to 3
        serverSendChunkedResponse(request3, true, "this", " is", " request", " 3");


        assertEquals("this is request 3", clientReceiveResponseBody());
        assertEquals("this is request 4", clientReceiveResponseBody());

        // Send/receive one more to make sure no buffering is done
        clientMakeRequest("/");
        SequencedHttpRequest request5 = serverReceiveRequest();
        serverSendChunkedResponse(request5, true, "this", " is", " request", " 5");
        assertEquals("this is request 5", clientReceiveResponseBody());
    }

    @Test
    public void backPressure() throws Exception {
        // Send
        clientMakeRequest("/1");
        SequencedHttpRequest request1 = serverReceiveRequest(); // inFlight = 1
        clientMakeRequest("/2");
        SequencedHttpRequest request2 = serverReceiveRequest(); // inFlight = 2
        clientMakeRequest("/3");
        SequencedHttpRequest request3 = serverReceiveRequest(); // inFlight = 3
        // This write will cause the high water mark to be reached
        clientMakeRequest("/4");
        SequencedHttpRequest request4 = serverReceiveRequest(); // inFlight = 4

        // High watermark is exceeded, next write should never be received
        clientMakeRequest("/5");
        assertNull(serverMessagesReceived.poll(200, TimeUnit.MILLISECONDS));

        // Until we reach the low water mark (2), the server still should not be receiving the messages
        serverSendChunkedResponse(request1, true, "1");
        assertEquals("1", clientReceiveResponseBody()); // inFlight = 3
        assertNull(serverMessagesReceived.poll(200, TimeUnit.MILLISECONDS));
        serverSendChunkedResponse(request2, true, "2");
        assertEquals("2", clientReceiveResponseBody()); // inFlight = 2

        // Since inFlight dropped to 2, we should now be able to receive the request on the server
        SequencedHttpRequest request5 = serverReceiveRequest(); // inFlight = 3

        clientMakeRequest("/6");
        SequencedHttpRequest request6 = serverReceiveRequest(); // inFlight = 4

        // High water mark exceeded again, write will be blocked
        clientMakeRequest("/7");
        assertNull(serverMessagesReceived.poll(200, TimeUnit.MILLISECONDS));

        serverSendChunkedResponse(request3, true, "3");
        assertEquals("3", clientReceiveResponseBody()); // inFlight = 3
        assertNull(serverMessagesReceived.poll(200, TimeUnit.MILLISECONDS));

        serverSendChunkedResponse(request4, true, "4");
        assertEquals("4", clientReceiveResponseBody()); // inFlight = 2

        SequencedHttpRequest request7 = serverReceiveRequest();  // inFlight = 3

        serverSendChunkedResponse(request5, true, "5");
        assertEquals("5", clientReceiveResponseBody()); // inFlight = 2
        serverSendChunkedResponse(request6, true, "6");
        assertEquals("6", clientReceiveResponseBody()); // inFlight = 1
        serverSendChunkedResponse(request7, true, "7");
        assertEquals("7", clientReceiveResponseBody()); // inFlight = 0
    }

    private void serverSendChunkedResponse(SequencedHttpRequest request, boolean await, String... messages) throws Exception {
        serverWrite(new SequencedOutboundMessage(request.getSequence(), createChunkedResponse()), await);
        for (String message: messages) {
            serverWrite(new SequencedOutboundMessage(request.getSequence(), createContent(message)), await);
        }
        serverWrite(new SequencedOutboundMessage(request.getSequence(), createLastContent()), await);
    }

    private void serverWrite(Object message, boolean await) throws Exception {
        ChannelFuture future = serverChannel.write(message);
        if (await) {
            serverChannel.flush();
            await(future);
        }
    }

    private void clientMakeRequest(String path) throws Exception {
        ChannelFuture future = clientChannel.write(createRequest(path));
        clientChannel.flush();
        await(future);
    }

    private FullHttpMessage createRequest(String path) {
        FullHttpMessage request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path);
        HttpHeaders.setContentLength(request, 0);
        return request;
    }
    private FullHttpResponse createResponse(String content) {
        ByteBuf buffer = Unpooled.copiedBuffer(content, Charset.defaultCharset());
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, buffer);
        HttpHeaders.setContentLength(response, buffer.readableBytes());
        return response;
    }
    private HttpResponse createChunkedResponse() {
        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
        HttpHeaders.setTransferEncodingChunked(response);
        return response;
    }
    private HttpContent createContent(String content) {
        return new DefaultHttpContent(Unpooled.copiedBuffer(content, Charset.defaultCharset()));
    }
    private HttpContent createLastContent() {
        return new DefaultLastHttpContent();
    }

    private String clientReceiveResponseBody() throws Exception {
        return clientReceiveResponse().content().toString(Charset.forName("utf-8"));
    }

    private FullHttpResponse clientReceiveResponse() throws Exception {
        return clientReceive(FullHttpResponse.class);
    }

    private SequencedHttpRequest serverReceiveRequest() throws Exception {
        SequencedHttpRequest request = serverReceive(SequencedHttpRequest.class);
        HttpObject message = request.getHttpRequest();
        while (!(message instanceof LastHttpContent)) {
            message = serverReceive(HttpContent.class);
        }
        return request;
    }

    private <T> T clientReceive(Class<T> clazz) throws Exception {
        Object message = clientMessagesReceived.poll(2, TimeUnit.SECONDS);
        assertNotNull(message);
        return clazz.cast(message);
    }

    private <T> T serverReceive(Class<T> clazz) throws Exception {
        Object message = serverMessagesReceived.poll(2, TimeUnit.SECONDS);
        assertNotNull(message);
        return clazz.cast(message);
    }

    static class ClientHandler extends ChannelInboundHandlerAdapter {
        private final Queue<Object> received;

        public ClientHandler(Queue<Object> received) {
            this.received = received;
        }

        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            received.add(msg);
        }
    }

    static class ServerHandler extends ChannelInboundHandlerAdapter {
        private final Queue<Object> received;
        private final Promise<Channel> channelPromise;

        public ServerHandler(Queue<Object> received, Promise<Channel> channelPromise) {
            this.received = received;
            this.channelPromise = channelPromise;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            received.add(msg);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            ctx.read();
            channelPromise.setSuccess(ctx.channel());
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            ctx.read();
        }
    }

    private static <F extends Future<T>, T> F await(F future) throws Exception {
        assertTrue(future.await(2000));
        return future;
    }
}
