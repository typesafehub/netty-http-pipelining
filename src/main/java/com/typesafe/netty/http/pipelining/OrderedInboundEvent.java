package com.typesafe.netty.http.pipelining;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import java.net.SocketAddress;

/**
 * Replacement of https://github.com/typesafehub/netty-http-pipelining/blob/master/src/main/java/com/typesafe/netty/http/pipelining/OrderedUpstreamMessageEvent.java
 *
 * Permits inbound message events to be ordered.
 */
public class OrderedInboundEvent {

    private final int sequence;
    private final Channel channel;
    private final Object message;
    private final SocketAddress remoteAddress;

    public OrderedInboundEvent(final int sequence, final Channel channel, final Object msg) {
        this.sequence = sequence;
        if (channel == null) {
            throw new NullPointerException("channel");
        }
        if (msg == null) {
            throw new NullPointerException("message");
        }
        this.channel = channel;
        this.message = msg;
        this.remoteAddress = channel.remoteAddress();
    }

    public int getSequence() {
        return sequence;
    }

    public Channel getChannel() {
        return channel;
    }

    public ChannelFuture getFuture() {
        return channel.newSucceededFuture();
    }

    public Object getMessage() {
        return message;
    }

    public SocketAddress getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public String toString() {
        if (getRemoteAddress() == getChannel().remoteAddress()) {
            return getChannel().toString() + " RECEIVED: " + getMessage();
                    //StringUtil.stripControlCharacters(getMessage());
        } else {
            return getChannel().toString() + " RECEIVED: " + getMessage() + " from " + getRemoteAddress();
                    //StringUtil.stripControlCharacters(getMessage()) + " from " +
                    //getRemoteAddress();
        }
    }

}
