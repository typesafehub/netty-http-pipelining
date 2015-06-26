package com.typesafe.netty.http.pipelining;

import io.netty.handler.codec.http.HttpRequest;

/**
 * A sequenced HTTP request.
 *
 * The sequence number should be used to send responses (and their data) wrapped in a SequencedOutboundMessage.
 */
public class SequencedHttpRequest {
    private final int sequence;
    private final HttpRequest httpRequest;

    /**
     * Create a sequenced HTTP request.
     *
     * @param sequence The sequence.
     * @param httpRequest The http request.
     */
    public SequencedHttpRequest(int sequence, HttpRequest httpRequest) {
        this.sequence = sequence;
        this.httpRequest = httpRequest;
    }

    /**
     * Get the sequence for this HTTP request.
     *
     * @return The sequence.
     */
    public int getSequence() {
        return sequence;
    }

    /**
     * Get the HTTP request.
     *
     * @return The HTTP request.
     */
    public HttpRequest getHttpRequest() {
        return httpRequest;
    }

    @Override
    public String toString() {
        return "SequencedHttpRequest{" +
                "sequence=" + sequence +
                ", httpRequest=" + httpRequest +
                '}';
    }
}
