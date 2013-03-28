package org.typesafe.netty.http.pipelining;

import org.jboss.netty.channel.*;

import java.net.SocketAddress;
import java.util.concurrent.Callable;

/**
 * Delegates operations to the underlying channel, but only once told to proceed
 */
public class HttpPipeliningChannel implements Channel {

    private final Channel delegate;
    private final ChannelFuture future = new DefaultChannelFuture(this, false);
    private Object attachment;

    public HttpPipeliningChannel(Channel delegate) {
        this.delegate = delegate;
    }

    public void proceed() {
        future.setSuccess();
    }

    public void cancel() {
        future.cancel();
    }

    @Override
    public Integer getId() {
        return delegate.getId();
    }

    @Override
    public ChannelFactory getFactory() {
        return delegate.getFactory();
    }

    @Override
    public Channel getParent() {
        return delegate.getParent();
    }

    @Override
    public ChannelConfig getConfig() {
        return delegate.getConfig();
    }

    @Override
    public ChannelPipeline getPipeline() {
        return delegate.getPipeline();
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public boolean isBound() {
        return delegate.isBound();
    }

    @Override
    public boolean isConnected() {
        return delegate.isConnected();
    }

    @Override
    public SocketAddress getLocalAddress() {
        return delegate.getLocalAddress();
    }

    @Override
    public SocketAddress getRemoteAddress() {
        return delegate.getRemoteAddress();
    }

    @Override
    public ChannelFuture write(final Object message) {
        return chainFutureTo(new Callable<ChannelFuture>() {
            @Override
            public ChannelFuture call() throws Exception {
                return delegate.write(message);
            }
        });
    }

    @Override
    public ChannelFuture write(final Object message, final SocketAddress remoteAddress) {
        return chainFutureTo(new Callable<ChannelFuture>() {
            @Override
            public ChannelFuture call() throws Exception {
                return delegate.write(message, remoteAddress);
            }
        });
    }

    @Override
    public ChannelFuture bind(final SocketAddress localAddress) {
        return chainFutureTo(new Callable<ChannelFuture>() {
            @Override
            public ChannelFuture call() throws Exception {
                return delegate.bind(localAddress);
            }
        });
    }

    @Override
    public ChannelFuture connect(final SocketAddress remoteAddress) {
        return chainFutureTo(new Callable<ChannelFuture>() {
            @Override
            public ChannelFuture call() throws Exception {
                return delegate.connect(remoteAddress);
            }
        });
    }

    @Override
    public ChannelFuture disconnect() {
        return chainFutureTo(new Callable<ChannelFuture>() {
            @Override
            public ChannelFuture call() throws Exception {
                return delegate.disconnect();
            }
        });
    }

    @Override
    public ChannelFuture unbind() {
        return chainFutureTo(new Callable<ChannelFuture>() {
            @Override
            public ChannelFuture call() throws Exception {
                return delegate.unbind();
            }
        });
    }

    @Override
    public ChannelFuture close() {
        return chainFutureTo(new Callable<ChannelFuture>() {
            @Override
            public ChannelFuture call() throws Exception {
                return delegate.close();
            }
        });
    }

    @Override
    public ChannelFuture getCloseFuture() {
        return chainFutureTo(new Callable<ChannelFuture>() {
            @Override
            public ChannelFuture call() throws Exception {
                return delegate.getCloseFuture();
            }
        });
    }

    @Override
    public int getInterestOps() {
        return delegate.getInterestOps();
    }

    @Override
    public boolean isReadable() {
        if (future.isDone()) {
            return delegate.isReadable();
        } else {
            return false;
        }
    }

    @Override
    public boolean isWritable() {
        if (future.isDone()) {
            return delegate.isWritable();
        } else {
            return false;
        }
    }

    @Override
    public ChannelFuture setInterestOps(final int interestOps) {
        return chainFutureTo(new Callable<ChannelFuture>() {
            @Override
            public ChannelFuture call() throws Exception {
                return delegate.setInterestOps(interestOps);
            }
        });
    }

    @Override
    public ChannelFuture setReadable(final boolean readable) {
        return chainFutureTo(new Callable<ChannelFuture>() {
            @Override
            public ChannelFuture call() throws Exception {
                return delegate.setReadable(readable);
            }
        });
    }

    @Override
    public Object getAttachment() {
        return attachment;
    }

    @Override
    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

    @Override
    public int compareTo(Channel channel) {
        return delegate.compareTo(channel);
    }

    private ChannelFuture chainFutureTo(final Callable<ChannelFuture> op) {
        final ChannelFuture chained = new DefaultChannelFuture(this, true);
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isCancelled()) {
                    chained.cancel();
                } else {
                    op.call().addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) throws Exception {
                            if (future.isSuccess()) {
                                chained.setSuccess();
                            } else if (future.isCancelled()) {
                                chained.cancel();
                            } else {
                                chained.setFailure(future.getCause());
                            }
                        }
                    });
                }
            }
        });
        return chained;
    }
}