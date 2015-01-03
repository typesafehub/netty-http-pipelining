package com.typesafe.netty.http.pipelining;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

/**
 * Replacement of https://github.com/typesafehub/netty-http-pipelining/blob/master/src/main/java/com/typesafe/netty/http/pipelining/OrderedDownstreamChannelEvent.java
 *
 * Permits outbound channel events to be ordered and signalled as to whether more are to come for a given sequence.
 */
public class OrderedOutboundEvent {

    private final int sequence;
    final boolean last;
    final OrderedInboundEvent orderedInboundEvent;
    final Object msg;

    public OrderedOutboundEvent(final int sequence, final OrderedInboundEvent orderedInboundEvent,
                                final boolean last, final Object msg) {
        this.sequence = sequence;
        this.last = last;
        this.orderedInboundEvent = orderedInboundEvent;
        this.msg = msg;
    }

    public int getSequence() {
        return sequence;
    }

    public Channel getChannel() {
        return orderedInboundEvent.getChannel();
    }

    public ChannelFuture getFuture() {
        return orderedInboundEvent.getFuture();
    }

    public OrderedInboundEvent getOrderedInboundEvent() {
        return orderedInboundEvent;
    }

    public Object getMessage() {
        return msg;
    }

    public boolean isLast() {
        return last;
    }
}
