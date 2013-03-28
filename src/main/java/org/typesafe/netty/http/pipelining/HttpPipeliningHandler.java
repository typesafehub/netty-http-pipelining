package org.typesafe.netty.http.pipelining;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their
 * corresponding requests. NOTE: A side effect of using this handler is that upstream HttpRequest objects will
 * cause the original message event to be effectively transformed into an OrderedUpstreamMessageEvent. Conversely
 * OrderedDownstreamMessageEvent objects are expected to be received for the correlating response objects.
 *
 * @author Christopher Hunt
 */
public class HttpPipeliningHandler extends SimpleChannelHandler {

    public static final int MAX_EVENTS_HELD = 10000;

    private final int maxEventsHeld;

    private int sequence;
    private int nextRequiredSequence;

    private final PriorityQueue<OrderedDownstreamMessageEvent> holdingQueue;

    public HttpPipeliningHandler() {
        this(MAX_EVENTS_HELD);
    }

    /**
     * @param maxEventsHeld the maximum number of message events that will be retained prior to aborting the channel
     *                      connection. This is required as events cannot queue up indefintely; we would run out of
     *                      memory if this was the case.
     */
    public HttpPipeliningHandler(final int maxEventsHeld) {
        this.maxEventsHeld = maxEventsHeld;

        holdingQueue = new PriorityQueue<OrderedDownstreamMessageEvent>(maxEventsHeld, new Comparator<OrderedDownstreamMessageEvent>() {
            @Override
            public int compare(OrderedDownstreamMessageEvent o1, OrderedDownstreamMessageEvent o2) {
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
        }
    }

    @Override
    public void writeRequested(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
        if (e instanceof OrderedDownstreamMessageEvent) {
            if (holdingQueue.size() < maxEventsHeld) {

                final OrderedDownstreamMessageEvent currentEvent = (OrderedDownstreamMessageEvent) e;
                holdingQueue.add(currentEvent);

                while (!holdingQueue.isEmpty() &&
                        holdingQueue.peek().getOrderedUpstreamMessageEvent().getSequence() == nextRequiredSequence) {
                    final OrderedDownstreamMessageEvent nextEvent = holdingQueue.poll();
                    ctx.sendDownstream(nextEvent);
                    if (nextEvent.isLast()) {
                        ++nextRequiredSequence;
                    }
                }

            } else {
                Channels.disconnect(e.getChannel());
            }
        }
    }
}
