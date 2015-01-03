package com.typesafe.netty.http.pipelining;

import io.netty.channel.*;
import io.netty.handler.codec.http.DefaultFullHttpRequest;

import java.util.*;

/**
 * Replacement of https://github.com/typesafehub/netty-http-pipelining/blob/master/src/main/java/com/typesafe/netty/http/pipelining/HttpPipeliningHandler.java
 *
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their
 * corresponding requests. NOTE: A side effect of using this handler is that upstream HttpRequest objects will
 * cause the original message event to be effectively transformed into an OrderedUpstreamMessageEvent. Conversely
 * OrderedDownstreamChannelEvent objects are expected to be received for the correlating response objects.
 *
 * @author Christopher Hunt
 */
public class HttpPipeliningHandler extends ChannelHandlerAdapter {

    public static final int INITIAL_EVENTS_HELD = 3;
    public static final int MAX_EVENTS_HELD = 10000;
    private final int maxEventsHeld;
    private int sequence;
    private int nextRequiredSequence;
    private int nextRequiredSubsequence;
    private final Queue<OrderedOutboundEvent> holdingQueue;

    public HttpPipeliningHandler() {
        this(MAX_EVENTS_HELD);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof DefaultFullHttpRequest) {
            ctx.fireChannelRead(new OrderedInboundEvent(sequence++, ctx.channel(), msg));
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    /**
     * @param maxEventsHeld the maximum number of channel events that will be retained prior to aborting the channel
     *                      connection. This is required as events cannot queue up indefintely; we would run out of
     *                      memory if this was the case.
     */
    public HttpPipeliningHandler(final int maxEventsHeld) {
        this.maxEventsHeld = maxEventsHeld;
        holdingQueue = new PriorityQueue<OrderedOutboundEvent>(INITIAL_EVENTS_HELD, new Comparator<OrderedOutboundEvent>() {
            @Override
            public int compare(OrderedOutboundEvent o1, OrderedOutboundEvent o2) {
                final int delta = o1.getOrderedInboundEvent().getSequence() - o2.getOrderedInboundEvent().getSequence();
                if (delta == 0) {
                    return o1.getSequence() - o2.getSequence();
                } else {
                    return delta;
                }
            }
        });
    }

    public int getMaxEventsHeld() {
        return maxEventsHeld;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof OrderedOutboundEvent) {
            boolean channelShouldClose = false;
            synchronized (holdingQueue) {
                if (holdingQueue.size() < maxEventsHeld) {
                    final OrderedOutboundEvent currentEvent = (OrderedOutboundEvent) msg;
                    holdingQueue.add(currentEvent);
                    while (!holdingQueue.isEmpty()) {
                        final OrderedOutboundEvent nextEvent = holdingQueue.peek();
                        if (nextEvent.getOrderedInboundEvent().getSequence() != nextRequiredSequence |
                                nextEvent.getSequence() != nextRequiredSubsequence) {
                            break;
                        }
                        holdingQueue.remove();
                        ctx.writeAndFlush(nextEvent.getMessage(), promise);
                        if (nextEvent.isLast()) {
                            ++nextRequiredSequence;
                            nextRequiredSubsequence = 0;
                        } else {
                            ++nextRequiredSubsequence;
                        }
                    }
                } else {
                    channelShouldClose = true;
                }
            }
            if (channelShouldClose) {
                ctx.close(promise);
            }
        } else {
            ctx.writeAndFlush(msg, promise);
        }
    }

    // ================== OLD Netty 3.x code =====================================
    /*@Override
    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e)
            throws Exception {
        if (e instanceof OrderedDownstreamChannelEvent) {
            boolean channelShouldClose = false;
            synchronized (holdingQueue) {
                if (holdingQueue.size() < maxEventsHeld) {
                    final OrderedDownstreamChannelEvent currentEvent = (OrderedDownstreamChannelEvent) e;
                    holdingQueue.add(currentEvent);
                    while (!holdingQueue.isEmpty()) {
                        final OrderedDownstreamChannelEvent nextEvent = holdingQueue.peek();
                        if (nextEvent.getOrderedUpstreamMessageEvent().getSequence() != nextRequiredSequence |
                                nextEvent.getSubsequence() != nextRequiredSubsequence) {
                            break;
                        }
                        holdingQueue.remove();
                        ctx.sendDownstream(nextEvent.getChannelEvent());
                        if (nextEvent.isLast()) {
                            ++nextRequiredSequence;
                            nextRequiredSubsequence = 0;
                        } else {
                            ++nextRequiredSubsequence;
                        }
                    }
                } else {
                    channelShouldClose = true;
                }
            }
            if (channelShouldClose) {
                Channels.close(e.getChannel());
            }
        } else {
            super.handleDownstream(ctx, e);
        }
    }

    @Override
    public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
        final Object msg = e.getMessage();
        if (msg instanceof HttpRequest) {
            ctx.sendUpstream(new OrderedUpstreamMessageEvent(sequence++, e.getChannel(), msg, e.getRemoteAddress()));
        } else {
            ctx.sendUpstream(e);
        }
    }*/
}
