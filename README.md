netty-http-pipelining
=====================

This library adds http pipelining capability to Netty.

Inserting the HttpPipeliningHandler into a pipeline will cause message events containing an HttpRequest to become transformed
into OrderedUpstreamMessageEvents. The OrderedUpstreamMessageEvent retains context such that a handler further upstream can
compaose it and reply with an OrderedDownstreamMessageEvent in any order and in parallel. The HttpPipeliningHandler will
ensure that http replies are sent back in the order that the http pipelining specification requires i.e. the order in which
replies are returned must correlate to the order in which requests are made.

The chunking of http replies is handled. Limits are also available within the handler to cap the buffering of replies
in order to avoid memory exhaustion.

Please refer to the HttpPipeliningHandlerTest for a comprehensive illustration of usage.