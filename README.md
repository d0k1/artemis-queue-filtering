# JMS queue filtering

This simple example shows a technique for dropping some messages from a queue of Apache ActiveMQ Artemis.

I wanted to avoid a lot of broken messages being processed, so I created this simple example.

This application connects to a broker, and receives messages one by one. When it receives a broken message the app just acknowledges it. Artemis removes acknowledged message from a queue. 
On the other hand, when the app receives a valid message, the app send it back to the queue.
 
