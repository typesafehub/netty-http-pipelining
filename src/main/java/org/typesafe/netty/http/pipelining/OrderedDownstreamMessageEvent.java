package org.typesafe.netty.http.pipelining;

import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.DownstreamMessageEvent;

/**
 * Permits downstream message events to be ordered and signalled as to whether more are to come for a given sequence.
 *
 * @author Christopher Hunt
 */
public class OrderedDownstreamMessageEvent extends DownstreamMessageEvent {

    final OrderedUpstreamMessageEvent oue;
    final int subsequence;
    final boolean last;

    /**
     * Convenience constructor signifying that this downstream event is the last one for the given sequence, and that
     * there is only one response.
     */
    public OrderedDownstreamMessageEvent(final OrderedUpstreamMessageEvent oe,
                                         final Object message) {
        this(oe, 0, true, message);
    }

    /**
     * @param oue         the OrderedUpstreamMessageEvent that this response is associated with
     * @param subsequence the sequence within the sequence
     * @param last        when set to true this indicates that there are no more responses to be received for the
     *                    original OrderedUpstreamMessageEvent
     */
    public OrderedDownstreamMessageEvent(final OrderedUpstreamMessageEvent oue, final int subsequence, boolean last,
                                         final Object message) {
        super(oue.getChannel(), Channels.future(oue.getChannel()), message, oue.getRemoteAddress());
        this.oue = oue;
        this.subsequence = subsequence;
        this.last = last;
    }

    public OrderedUpstreamMessageEvent getOrderedUpstreamMessageEvent() {
        return oue;
    }

    public int getSubsequence() {
        return subsequence;
    }

    public boolean isLast() {
        return last;
    }

}
