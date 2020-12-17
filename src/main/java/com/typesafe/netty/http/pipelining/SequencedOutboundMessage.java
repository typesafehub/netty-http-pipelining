package com.typesafe.netty.http.pipelining;

import io.netty.handler.codec.http.HttpObject;

/**
 * A sequenced outbound message.
 *
 * This allows a handler to send one to many outbound messages associated with a sequence number from a
 * {@link SequencedHttpRequest}, such that all the outbound messages for a particular sequence number are sent before
 * any of the messages for the next sequence number are sent.
 *
 * For each SequencedHttpRequest received, downstream handlers are required to send zero to many messages that don't
 * extend LastHttpContent, followed by exactly one message that does extend LastHttpContent.
 */
public class SequencedOutboundMessage {
    private final int sequence;
    private final HttpObject message;

    /**
     * Create a sequenced outbound message.
     *
     * @param sequence The sequence of the {@link SequencedHttpRequest} that this outbound message is associated with.
     * @param message The message.
     */
    public SequencedOutboundMessage(int sequence, HttpObject message) {
        this.sequence = sequence;
        this.message = message;
    }

    /**
     * Get the sequence.
     *
     * @return The sequence.
     */
    public int getSequence() {
        return sequence;
    }

    /**
     * Get the message.
     *
     * @return The message.
     */
    public HttpObject getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "SequencedOutboundMessage{" +
                "sequence=" + sequence +
                ", message=" + message +
                '}';
    }
}
