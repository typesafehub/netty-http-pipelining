netty-http-pipelining
=====================

This library adds http pipelining capability to Netty.

Inserting the HttpPipeliningHandler into a pipeline will cause message events containing an HttpRequest to become transformed
into SequencedHttpRequest. The SequencedHttpRequest retains context such that a handler further upstream can
compose it and reply with an SequencedOutboundMessage in any order and in parallel. The HttpPipeliningHandler will
ensure that http replies are sent back in the order that the http pipelining specification requires i.e. the order in which
replies are returned must correlate to the order in which requests are made.
