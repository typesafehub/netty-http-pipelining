package org.typesafe.netty.http.pipelining;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import java.util.LinkedList;

public class HttpPipeliningHandler implements ChannelUpstreamHandler, ChannelDownstreamHandler {

    private final LinkedList<Channel> channelQueue = new LinkedList<Channel>();
    private long currentResponseContentLength = -1;
    private boolean isChunked;

    @Override
    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        if (e instanceof MessageEvent) {
            MessageEvent msgEvent = (MessageEvent) e;
            Object message = msgEvent.getMessage();
            if (message instanceof HttpResponse) {
                HttpResponse response = (HttpResponse) message;
                if (response.getStatus() == HttpResponseStatus.NO_CONTENT || response.getStatus() == HttpResponseStatus.NOT_MODIFIED) {
                    currentResponseContentLength = 0;
                } else {
                    currentResponseContentLength = HttpHeaders.getContentLength(response, -1);
                }
                isChunked = response.isChunked();
                if (!isChunked) {
                    observeContent(response.getContent(), e.getFuture());
                }
            } else if (message instanceof HttpChunk) {
                HttpChunk chunk = (HttpChunk) message;
                if (isChunked) {
                    if (chunk.isLast()) {
                        popChannelQueue(e.getFuture());
                    }
                } else {
                    observeContent(chunk.getContent(), e.getFuture());
                }
            } else if (message instanceof ChannelBuffer) {
                observeContent((ChannelBuffer) message, e.getFuture());
            }
        }
        ctx.sendDownstream(e);
    }

    private void observeContent(ChannelBuffer buffer, ChannelFuture future) {
        if (currentResponseContentLength >= 0) {
            currentResponseContentLength -= buffer.readableBytes();
            if (currentResponseContentLength <= 0) {
                popChannelQueue(future);
                currentResponseContentLength = -1;
            }
        }
    }

    private void popChannelQueue(ChannelFuture future) {
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                Channel next;
                synchronized (channelQueue) {
                    channelQueue.pop();
                    next = channelQueue.peek();
                }
                if (next instanceof HttpPipeliningChannel) {
                    if (future.isCancelled()) {
                        ((HttpPipeliningChannel) next).cancel();
                    } else {
                        ((HttpPipeliningChannel) next).proceed();
                    }
                }
            }
        });
    }

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        // A messages has come from upstream (ie, part of a request)
        if (e instanceof MessageEvent) {
            MessageEvent msgEvent = (MessageEvent) e;
            Object msg = msgEvent.getMessage();
            if (msg instanceof HttpRequest) {
                synchronized (channelQueue) {
                    if (!channelQueue.isEmpty()) {
                        // This request is pipelined behind another
                        channelQueue.add(new HttpPipeliningChannel(e.getChannel()));
                    } else {
                        // No pipelining, so no need to use the pipelining channel
                        channelQueue.add(e.getChannel());
                    }
                }
            }
            ctx.sendUpstream(new UpstreamMessageEvent(channelQueue.getLast(), msg, msgEvent.getRemoteAddress()));
        } else {
            ctx.sendUpstream(e);
        }
    }
}