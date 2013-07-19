package com.typesafe.netty.http.pipelining;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their
 * corresponding requests. NOTE: A side effect of using this handler is that upstream HttpRequest objects will
 * cause the original message event to be effectively transformed into an OrderedUpstreamMessageEvent. Conversely
 * OrderedDownstreamChannelEvent objects are expected to be received for the correlating response objects.
 *
 * @author Christopher Hunt
 */
public class HttpPipeliningHandler extends SimpleChannelHandler {

    public static final int MAX_EVENTS_HELD = 10000;

    private final int maxEventsHeld;

    private int sequence;
    private int nextRequiredSequence;
    private int nextRequiredSubsequence;

    private final Queue<OrderedDownstreamChannelEvent> holdingQueue;

    public HttpPipeliningHandler() {
        this(MAX_EVENTS_HELD);
    }

    /**
     * @param maxEventsHeld the maximum number of channel events that will be retained prior to aborting the channel
     *                      connection. This is required as events cannot queue up indefintely; we would run out of
     *                      memory if this was the case.
     */
    public HttpPipeliningHandler(final int maxEventsHeld) {
        this.maxEventsHeld = maxEventsHeld;

        holdingQueue = new PriorityQueue<OrderedDownstreamChannelEvent>(maxEventsHeld, new Comparator<OrderedDownstreamChannelEvent>() {
            @Override
            public int compare(OrderedDownstreamChannelEvent o1, OrderedDownstreamChannelEvent o2) {
                final int delta = o1.getOrderedUpstreamMessageEvent().getSequence() - o2.getOrderedUpstreamMessageEvent().getSequence();
                if (delta == 0) {
                    return o1.getSubsequence() - o2.getSubsequence();
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
    public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
        final Object msg = e.getMessage();
        if (msg instanceof HttpRequest) {
            ctx.sendUpstream(new OrderedUpstreamMessageEvent(sequence++, e.getChannel(), msg, e.getRemoteAddress()));
        } else {
            ctx.sendUpstream(e);
        }
    }

    @Override
    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e)
            throws Exception {
        if (e instanceof OrderedDownstreamChannelEvent) {
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
                    Channels.close(e.getChannel());
                }
            }
        } else {
            super.handleDownstream(ctx, e);
        }
    }

}
