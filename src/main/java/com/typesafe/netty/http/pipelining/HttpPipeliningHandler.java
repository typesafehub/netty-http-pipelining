package com.typesafe.netty.http.pipelining;


import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;

import java.util.*;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their
 * corresponding requests.
 *
 * Each incoming request is assigned a sequence number, which is provided in the wrapping {@link SequencedHttpRequest}.
 * When a handler writes a response and a response body, it must wrap each of those messages in a
 * {@link SequencedOutboundMessage} with the corresponding sequence from that request.  It must send at least one
 * message in response to the request, and the last message in the sequence must implement LastHttpContent.
 *
 * If messages are sent after the last message is sent, the behaviour is undefined.
 *
 * There is no limit to the amount of messages that this handler will buffer for a particular sequence number.  It is
 * the responsibility of the handler sending the outbound messages to handle back pressure via promises associated
 * with each write event - if this is done, the buffering will be inherently bounded by back pressure.
 *
 * This handler does however put a bound on the maximum number of in flight requests that it will handle, configured by
 * inFlightRequestsLowWatermark and inFlightRequestsHighWatermark. When the high watermark is exceeded, the handler will
 * push back on the client. When the low watermark is reached, the handler will start reading again.  This back pressure
 * mechanism only works if ChannelOptions.AUTO_READ is false.
 *
 * This back pressure is implemented by blocking channelReadComplete events, so assumes that the following handlers
 * will not request reading unless they receive this event.  Note that the handler does nothing to actually block
 * incoming requests when the high watermark is reached, it only pushes back on the TCP connection.  If there are more
 * requests in the TCP buffer before this back pressure takes effect, these requests will still be sent to the following
 * handlers.
 *
 * If channelReadComplete is invoked while the high watermark is reached, then when the low watermark is reached, this
 * will be fired again, to signal demand.
 */
public class HttpPipeliningHandler extends ChannelDuplexHandler {

    /**
     * The sequence of received HTTP requests.
     */
    private int receiveSequence = 0;

    /**
     * The currently sending sequence of HTTP requests.
     */
    private int currentlySendingSequence = 1;

    /**
     * Whether the high watermark has been exceeded.
     */
    private boolean highWatermarkReached = false;

    /**
     * If the high watermark has been exceeded, whether a channel read complete has occurred.
     */
    private boolean channelReadCompleteOccurred = false;

    /**
     * Whether the channel is inactive - if it's inactive, we should not buffer events to prevent leaks.
     */
    private boolean inactive = true;

    /**
     * A write message with a promise of when it's written.
     */
    private static class WriteMessage {
        /**
         * The written message.
         */
        final SequencedOutboundMessage message;

        /**
         * The future that is redeemed once the message is written.
         */
        final ChannelPromise promise;

        public WriteMessage(SequencedOutboundMessage message, ChannelPromise promise) {
            this.message = message;
            this.promise = promise;
        }
    }

    /**
     * The buffered events, by sequence
     */
    private final IntObjectMap<List<WriteMessage>> bufferedEvents = new IntObjectHashMap<>();

    private final int inFlightRequestsLowWatermark;
    private final int inFlightRequestsHighWatermark;

    /**
     * Create the pipelining handler with low and high watermarks of 2 and 4.
     */
    public HttpPipeliningHandler() {
        this(2, 4);
    }

    /**
     * Create the pipelining handler.
     *
     * @param inFlightRequestsLowWatermark The low watermark for in flight requests, where, if the high watermark has
     *                                     been exceeded, the handler will start reading again. Must be at least 0.
     * @param inFlightRequestsHighWatermark The high watermark, once in flight requests has exceeded this, the handler
     *                                      will stop reading, pushing back on the client.  Must be greater than the
     *                                      low watermark.
     */
    public HttpPipeliningHandler(int inFlightRequestsLowWatermark, int inFlightRequestsHighWatermark) {
        if (inFlightRequestsLowWatermark < 0) {
            throw new IllegalArgumentException("inFlightRequestsLowWatermark must be an least 0, was " + inFlightRequestsLowWatermark);
        }
        if (inFlightRequestsHighWatermark <= inFlightRequestsLowWatermark) {
            throw new IllegalArgumentException("inFlightRequestsHighWatermark must be greater than inFlightRequestsLowWatermark, but was " + inFlightRequestsHighWatermark);
        }
        this.inFlightRequestsLowWatermark = inFlightRequestsLowWatermark;
        this.inFlightRequestsHighWatermark = inFlightRequestsHighWatermark;
    }

    private int inFlight() {
        return receiveSequence - currentlySendingSequence + 1;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        // Only forward read complete if we haven't exceeded the high watermark
        if (!highWatermarkReached) {
            ctx.fireChannelReadComplete();
        } else {
            // Store the fact that read complete has been requested.
            channelReadCompleteOccurred = true;
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Object toSend = msg;

        // Wrap message in sequenced if it needs to be
        if (msg instanceof HttpRequest) {
            receiveSequence++;
            HttpRequest request = (HttpRequest) msg;
            toSend = new SequencedHttpRequest(receiveSequence, request);
        }

        // If we've reached the end of an http request, and we're at or over the high watermark,
        // set it to reached.
        if (msg instanceof LastHttpContent && inFlight() >= inFlightRequestsHighWatermark) {
            highWatermarkReached = true;
        }

        ctx.fireChannelRead(toSend);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof SequencedOutboundMessage) {
            SequencedOutboundMessage sequenced = (SequencedOutboundMessage) msg;

            writeInSequence(ctx, sequenced, promise);
        } else {
            ctx.write(msg, promise);
        }
    }

    private void progressToNextSendingSequence(ChannelHandlerContext ctx) {
        currentlySendingSequence++;

        int inFlight = this.inFlight();
        // If we're now at the low water mark, set it to false
        if (highWatermarkReached && inFlight == inFlightRequestsLowWatermark) {
            highWatermarkReached = false;

            // Check if, while we were over the high watermark, a channel read had occurred
            // that we blocked
            if (channelReadCompleteOccurred) {
                // Send it on
                ctx.fireChannelReadComplete();
                channelReadCompleteOccurred = false;
            }
        }
    }

    /**
     * Write the next sequences, if buffered.
     */
    private void flushNextSequences(ChannelHandlerContext ctx) {
        progressToNextSendingSequence(ctx);

        List<WriteMessage> toFlush = bufferedEvents.get(currentlySendingSequence);

        // Loop while we still have a sequence to flush
        while (toFlush != null) {

            bufferedEvents.remove(currentlySendingSequence);

            WriteMessage lastWritten = null;

            // Flush each event
            for (WriteMessage message: toFlush) {
                ctx.write(message.message.getMessage(), message.promise);
                lastWritten = message;
            }

            // If the last message that we wrote was the last message for that sequence,
            // then increment the sequence and maybe get the next sequence from the buffer.
            if (lastWritten != null && lastWritten.message.getMessage() instanceof LastHttpContent) {
                progressToNextSendingSequence(ctx);
                toFlush = bufferedEvents.get(currentlySendingSequence);
            } else {
                toFlush = null;
            }
        }
    }

    /**
     * Write the message in sequence.
     */
    private void writeInSequence(ChannelHandlerContext ctx, SequencedOutboundMessage sequenced, ChannelPromise promise) {
        if (sequenced.getSequence() == currentlySendingSequence) {
            ctx.write(sequenced.getMessage(), promise);
            if (sequenced.getMessage() instanceof LastHttpContent) {
                flushNextSequences(ctx);
            }
        } else {
            if (!inactive) {
                List<WriteMessage> sequenceBuffer = bufferedEvents.get(sequenced.getSequence());
                if (sequenceBuffer == null) {
                    sequenceBuffer = new ArrayList<>();
                    bufferedEvents.put(sequenced.getSequence(), sequenceBuffer);
                }
                sequenceBuffer.add(new WriteMessage(sequenced, promise));
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cleanup();
        inactive = true;
        super.channelInactive(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        cleanup();
        super.handlerRemoved(ctx);
    }

    private void cleanup() {
        for (List<WriteMessage> messages: bufferedEvents.values()) {
            for (WriteMessage message: messages) {
                ReferenceCountUtil.release(message.message.getMessage());
            }
        }
    }
}
