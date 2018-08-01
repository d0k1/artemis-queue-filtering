package com.focusit.jms;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class App
{
    /**
     * Create a connectionFactory to communicate with a remote broker
     */
    private static ActiveMQConnectionFactory createRemoteConnectionFactory()
    {
        final Map<String, Object> nettyParams = new HashMap<>();

        // connection params: host and port of artemis acceptor
        nettyParams.put("host", "secret.host");
        nettyParams.put("port", "123456");
        final ActiveMQConnectionFactory result = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                new TransportConfiguration(NettyConnectorFactory.class.getName(), nettyParams));

        // artemis credentials
        result.setUser("guest");
        result.setPassword("guest");

        return result;
    }

    public static void main(String[] args) throws Exception
    {
        final ActiveMQConnectionFactory connectionFactory = createRemoteConnectionFactory();
        final ServerLocator serverLocator = connectionFactory.getServerLocator();
        serverLocator.setBlockOnAcknowledge(true);

        // name of a queue to process
        String queue = "Queue.Secret";

        // Connecting
        ClientSessionFactory factory = serverLocator.createSessionFactory();
        ClientSession session = factory.createSession();
        // start!
        session.start();

        System.out.println("Session " + session.toString());

        // producer / consumer on the queue
        ClientProducer producer = session.createProducer(queue);
        ClientConsumer consumer = session.createConsumer(queue);

        // receive one message with 5000ms timeout
        ClientMessage msg = consumer.receive(5000);

        int counter = 25000;
        int position = 0;
        int dropped = 0;
        // if message has been received
        while (msg != null)
        {
            String body = msg.toString();
            // check message body to decide whether it must be dropped or must be processed a bit later
            if (body.contains("The secret condition"))
            {
                // print it out
                System.out.println("MSG: " + body);
                // drop it
                msg.acknowledge();
                dropped++;
                System.out.println("Dropping message " + msg.getMessageID() + ". Total dropped " + dropped);
            }
            else
            {
                msg.acknowledge();
                // put the message back
                producer.send(msg);
            }
            ++position;
            if (position >= counter)
            {
                break;
            }
            System.out.println("Processed " + position + " of " + counter);
            msg = consumer.receive(5000);
        }
        consumer.close();
        session.close();
        serverLocator.close();
    }
}
