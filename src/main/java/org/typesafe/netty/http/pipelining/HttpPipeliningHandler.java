package org.typesafe.netty.http.pipelining;

import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioSocketChannelConfig;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.queue.BufferedWriteHandler;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their
 * corresponding requests.
 *
 * @author Christopher Hunt
 */
public class HttpPipeliningHandler extends BufferedWriteHandler {

    private int sequence;
    private int nextRequiredSequence;

    private final PriorityQueue<OrderedDownstreamMessageEvent> holdingQueue =
            new PriorityQueue<OrderedDownstreamMessageEvent>(100, new Comparator<OrderedDownstreamMessageEvent>() {
                @Override
                public int compare(OrderedDownstreamMessageEvent o1, OrderedDownstreamMessageEvent o2) {
                    return o1.getSequence() - o2.getSequence();
                }
            });

    public HttpPipeliningHandler() {
        super(true);
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        final ChannelConfig cfg = e.getChannel().getConfig();
        if (cfg instanceof NioSocketChannelConfig) {
            // Lower the watermark to increase the chance of consolidation.
            final NioSocketChannelConfig nioCfg = (NioSocketChannelConfig) cfg;
            nioCfg.setWriteBufferLowWaterMark(0);
        }
        super.channelOpen(ctx, e);
    }

    @Override
    public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
        final Object msg = e.getMessage();
        if (msg instanceof HttpRequest) {
            ctx.sendUpstream(new OrderedUpstreamMessageEvent(sequence++, e.getChannel(), msg, e.getRemoteAddress()));
        }
    }

    @Override
    public synchronized void writeRequested(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
        if (e instanceof OrderedDownstreamMessageEvent) {
            final OrderedDownstreamMessageEvent currentEvent = (OrderedDownstreamMessageEvent) e;
            holdingQueue.add(currentEvent);

            while (!holdingQueue.isEmpty() && holdingQueue.peek().getSequence() == nextRequiredSequence) {
                final OrderedDownstreamMessageEvent nextEvent = holdingQueue.poll();
                super.writeRequested(ctx, nextEvent);
                if (nextEvent.isLast()) {
                    ++nextRequiredSequence;
                }
            }

            if (e.getChannel().isWritable()) {
                flush();
            }
        }
    }

    @Override
    public void channelInterestChanged(final ChannelHandlerContext ctx, final ChannelStateEvent e) {
        if (e.getChannel().isWritable()) {
            flush();
        }
    }
}
