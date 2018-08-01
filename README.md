# JMS queue filtering

This simple example shows a technique about how to drop some messages from a queue of Apache ActiveMQ Artemis.

I wanted to avoid a lot of broken messages being processed so I created this simple example.
This application connects to a broker, and received message one by one. When it receives a broken message this app just acknowledge it. Artemis removes from a queue acknowledged message. In other case, when the app receives a valid message the app acknowledges a message and send it back to the queue.
 
