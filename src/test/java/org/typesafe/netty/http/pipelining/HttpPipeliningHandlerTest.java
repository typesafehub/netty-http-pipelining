package org.typesafe.netty.http.pipelining;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.jboss.netty.buffer.ChannelBuffers.copiedBuffer;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Values.KEEP_ALIVE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.jboss.netty.util.CharsetUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HttpPipeliningHandlerTest {

    private static final long RESPONSE_TIMEOUT = 1000L;
    private static final long CONNECTION_TIMEOUT = 1000L;
    private static final String CONTENT_TYPE_TEXT = "text/plain; charset=UTF-8";
    private static final InetSocketAddress HOST_ADDR = new InetSocketAddress("127.0.0.1", 9080);
    private static final String PATH1 = "/1";
    private static final String PATH2 = "/2";
    private static final String SOME_RESPONSE_TEXT = "some response for ";

    private ClientBootstrap clientBootstrap;
    private ServerBootstrap serverBootstrap;

    private CountDownLatch responsesIn;
    private final List<String> responses = new ArrayList<String>(2);

    @Before
    public void setUp() {
        clientBootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                        Executors.newSingleThreadExecutor(),
                        Executors.newSingleThreadExecutor()));

        clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(
                        new HttpClientCodec(),
                        new ClientHandler()
                );
            }
        });

        serverBootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

        serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(
                        new HttpRequestDecoder(),
                        new HttpResponseEncoder(),
                        new HttpPipeliningHandler(),
                        new ServerHandler()
                );
            }
        });

        serverBootstrap.bind(HOST_ADDR);
    }

    @After
    public void shutDown() {
        serverBootstrap.shutdown();
        serverBootstrap.releaseExternalResources();
        clientBootstrap.shutdown();
        clientBootstrap.releaseExternalResources();
    }

    @Test
    public void shouldReturnMessagesInOrder() throws InterruptedException {
        responsesIn = new CountDownLatch(2);
        responses.clear();

        final ChannelFuture connectionFuture = clientBootstrap.connect(HOST_ADDR);

        assertTrue(connectionFuture.await(CONNECTION_TIMEOUT));
        final Channel clientChannel = connectionFuture.getChannel();

        final HttpRequest request1 = new DefaultHttpRequest(
                HTTP_1_1, HttpMethod.GET, PATH1);
        request1.setHeader(HOST, HOST_ADDR.toString());

        final HttpRequest request2 = new DefaultHttpRequest(
                HTTP_1_1, HttpMethod.GET, PATH2);
        request2.setHeader(HOST, HOST_ADDR.toString());

        clientChannel.write(request1);
        clientChannel.write(request2);

        responsesIn.await(RESPONSE_TIMEOUT, MILLISECONDS);

        assertEquals(SOME_RESPONSE_TEXT + PATH1, responses.get(0));
        assertEquals(SOME_RESPONSE_TEXT + PATH2, responses.get(1));
    }

    public class ClientHandler extends SimpleChannelUpstreamHandler {
        @Override
        public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
            final HttpResponse response = (HttpResponse) e.getMessage();
            responses.add(response.getContent().toString(UTF_8));
            responsesIn.countDown();
        }
    }

    public static class ServerHandler extends SimpleChannelUpstreamHandler {
        ChannelFuture path1ResponseFuture;

        @Override
        public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) throws InterruptedException {
            final HttpRequest request = (HttpRequest) e.getMessage();

            final int sequence = ((OrderedUpstreamMessageEvent) e).getSequence();
            final String uri = request.getUri();

            if (request.getUri().equals(PATH1)) {
                path1ResponseFuture = Channels.future(e.getChannel());
                path1ResponseFuture.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture channelFuture) throws Exception {
                        final HttpResponse response = createResponse(uri);
                        ctx.sendDownstream(new OrderedDownstreamMessageEvent(sequence, ctx.getChannel(),
                                Channels.future(ctx.getChannel()), response, e.getRemoteAddress()));
                    }
                });
            } else {
                final HttpResponse response = createResponse(uri);
                ctx.sendDownstream(new OrderedDownstreamMessageEvent(sequence, ctx.getChannel(),
                        Channels.future(ctx.getChannel()), response, e.getRemoteAddress()));
                path1ResponseFuture.setSuccess();
            }
        }

        private static HttpResponse createResponse(final String uri) {
            final HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
            response.setContent(copiedBuffer(SOME_RESPONSE_TEXT + uri, UTF_8));
            response.setHeader(CONTENT_TYPE, CONTENT_TYPE_TEXT);
            response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());
            response.setHeader(CONNECTION, KEEP_ALIVE);
            return response;
        }
    }
}
