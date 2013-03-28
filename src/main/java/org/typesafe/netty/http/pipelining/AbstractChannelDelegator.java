package org.typesafe.netty.http.pipelining;

import org.jboss.netty.channel.*;

import java.net.SocketAddress;

/**
 * Provides a base class so that channels can be wrapped and overloaded with additional behaviour.
 *
 * @author Christopher Hunt
 */
public abstract class AbstractChannelDelegator implements Channel {
    private final Channel source;

    protected AbstractChannelDelegator(final Channel source) {
        this.source = source;
    }

    @Override
    public Integer getId() {
        return source.getId();
    }

    @Override
    public ChannelFactory getFactory() {
        return source.getFactory();
    }

    @Override
    public Channel getParent() {
        return source.getParent();
    }

    @Override
    public ChannelConfig getConfig() {
        return source.getConfig();
    }

    @Override
    public ChannelPipeline getPipeline() {
        return source.getPipeline();
    }

    @Override
    public boolean isOpen() {
        return source.isOpen();
    }

    @Override
    public boolean isBound() {
        return source.isBound();
    }

    @Override
    public boolean isConnected() {
        return source.isConnected();
    }

    @Override
    public SocketAddress getLocalAddress() {
        return source.getLocalAddress();
    }

    @Override
    public SocketAddress getRemoteAddress() {
        return source.getRemoteAddress();
    }

    @Override
    public ChannelFuture write(final Object message) {
        return source.write(message);
    }

    @Override
    public ChannelFuture write(final Object message, SocketAddress remoteAddress) {
        return source.write(message, remoteAddress);
    }

    @Override
    public ChannelFuture bind(final SocketAddress localAddress) {
        return source.bind(localAddress);
    }

    @Override
    public ChannelFuture connect(final SocketAddress remoteAddress) {
        return source.connect(remoteAddress);
    }

    @Override
    public ChannelFuture disconnect() {
        return source.disconnect();
    }

    @Override
    public ChannelFuture unbind() {
        return source.unbind();
    }

    @Override
    public ChannelFuture close() {
        return source.close();
    }

    @Override
    public ChannelFuture getCloseFuture() {
        return source.getCloseFuture();
    }

    @Override
    public int getInterestOps() {
        return source.getInterestOps();
    }

    @Override
    public boolean isReadable() {
        return source.isReadable();
    }

    @Override
    public boolean isWritable() {
        return source.isWritable();
    }

    @Override
    public ChannelFuture setInterestOps(final int interestOps) {
        return source.setInterestOps(interestOps);
    }

    @Override
    public ChannelFuture setReadable(final boolean readable) {
        return source.setReadable(readable);
    }

    @Override
    public Object getAttachment() {
        return source.getAttachment();
    }

    @Override
    public void setAttachment(final Object attachment) {
        source.setAttachment(attachment);
    }

    @Override
    public int compareTo(final Channel o) {
        return source.compareTo(o);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final AbstractChannelDelegator that = (AbstractChannelDelegator) o;

        if (source != null ? !source.equals(that.source) : that.source != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return source != null ? source.hashCode() : 0;
    }
}
