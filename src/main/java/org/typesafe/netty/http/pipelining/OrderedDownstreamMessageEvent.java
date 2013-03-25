package org.typesafe.netty.http.pipelining;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.DownstreamMessageEvent;

import java.net.SocketAddress;

/**
 * Permits downstream message events to be ordered.
 *
 * @author Christopher Hunt
 */
public class OrderedDownstreamMessageEvent extends DownstreamMessageEvent {
    final int sequence;

    public OrderedDownstreamMessageEvent(final int sequence, final Channel channel, final ChannelFuture future, final Object message, final SocketAddress remoteAddress) {
        super(channel, future, message, remoteAddress);
        this.sequence = sequence;
    }

    public int getSequence() {
        return sequence;
    }

}
