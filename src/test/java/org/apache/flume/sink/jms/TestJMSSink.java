/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink.jms;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.sink.jms.JMSSink;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJMSSink {

  private static final Logger logger = LoggerFactory
      .getLogger(TestJMSSink.class);

  private final static String ACTIVEMQ_INITIAL_CONTEXT_FACTORY = "org.apache.activemq.jndi.ActiveMQInitialContextFactory";
  private final static String ACTIVEMQ_BROKER_PARAMETERS = "broker.persistent=false";
  private final static int MAX_ARRAY_SIZE = 256;

  /**
   * Checks a queue can be instantiated
   */
  @Test
  public void testQueueLifecycle() {
    JMSSink sink = new JMSSink();

    Context context = new Context();
    context.put("initialcontextfactory", ACTIVEMQ_INITIAL_CONTEXT_FACTORY);
    context.put("brokerurl", "vm://testQueueLifecycle?"
        + ACTIVEMQ_BROKER_PARAMETERS);
    context.put("destination", "/testQueueLifecycle/queue");
    context.put("destinationtype", "queue");
    Configurables.configure(sink, context);

    sink.setChannel(new MemoryChannel());

    sink.start();
    sink.stop();
  }

  /**
   * Checks a topic can be instantiated
   */
  @Test
  public void testTopicLifecycle() {
    JMSSink sink = new JMSSink();

    Context context = new Context();
    context.put("initialcontextfactory", ACTIVEMQ_INITIAL_CONTEXT_FACTORY);
    context.put("brokerurl", "vm://testTopicLifecycle?"
        + ACTIVEMQ_BROKER_PARAMETERS);
    context.put("destination", "/testTopicLifecycle/queue");
    context.put("destinationtype", "topic");
    Configurables.configure(sink, context);

    sink.setChannel(new MemoryChannel());

    sink.start();
    sink.stop();
  }

  /**
   * This test will just send some messages over an embedded ActiveMQ instance
   * and check they can be received through JMS
   *
   * @throws JMSException
   * @throws EventDeliveryException
   */
  @Test
  public void testSimple() throws JMSException, EventDeliveryException {

    final String ACTIVE_MQ_BROKER_URL = "vm://testSimple?"
        + ACTIVEMQ_BROKER_PARAMETERS;
    final String ACTIVE_MQ_QUEUE = "/testSimple/queue";
    final Integer NUM_MESSAGES = 10;

    JMSSink sink = new JMSSink();

    Context context = new Context();
    context.put("initialcontextfactory", ACTIVEMQ_INITIAL_CONTEXT_FACTORY);
    context.put("brokerurl", ACTIVE_MQ_BROKER_URL);
    context.put("destination", ACTIVE_MQ_QUEUE);
    context.put("destinationtype", "queue");
    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    for (int i = 0; i < NUM_MESSAGES; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      Event event = new SimpleEvent();

      // Set some headers
      event.getHeaders().put(String.valueOf(i), i + "/" + i);

      // Set its body
      String bodyStr = "This is message number " + i;
      event.setBody(bodyStr.getBytes());

      channel.put(event);
      txn.commit();
      txn.close();

      // execute sink to process the events
      sink.process();
    }
    sink.stop();


    /**
     * Now connecting to the broker and confirming messages have been
     * sent to the broker
     */
    ConnectionFactory factory = new ActiveMQConnectionFactory(
        ACTIVE_MQ_BROKER_URL);
    Connection connection = factory.createConnection();
    connection.start();

    Session session = connection.createSession(true,
        Session.AUTO_ACKNOWLEDGE);
    Destination destination = session.createQueue(ACTIVE_MQ_QUEUE);
    MessageConsumer consumer = session.createConsumer(destination);

    int maxTry = 3;
    Integer messagesReceivedCount = 0;
    while (messagesReceivedCount < NUM_MESSAGES) {
      int tryReceive = 0;
      Message message = null;

      do {
        message = consumer.receive(1500);

        tryReceive++;
      } while (message == null && tryReceive <= maxTry);

      assertNotNull("Failed to receive message", message);
      assertTrue("Message received is not a BytesMessage",
          message instanceof BytesMessage);

      assertTrue("Message " + messagesReceivedCount
          + " is missing header [" + messagesReceivedCount + "]",
          message.propertyExists(messagesReceivedCount.toString()));

      String propertyValue = message
          .getStringProperty(messagesReceivedCount.toString());
      assertEquals("Message header property has a wrong value",
          messagesReceivedCount + "/" + messagesReceivedCount,
          propertyValue);

      BytesMessage bytesMessage = (BytesMessage) message;
      long messageLength = bytesMessage.getBodyLength();

      assertTrue("Content of message is larger than ever expected",
          messageLength < MAX_ARRAY_SIZE && messageLength > 0);

      byte[] content = new byte[(int) messageLength];
      bytesMessage.readBytes(content);
      assertEquals("Body of message is not correct",
          "This is message number " + messagesReceivedCount,
          new String(content));

      messagesReceivedCount++;
      session.commit();
    }

    assertEquals("Didn't receive the right amount of messages",
        NUM_MESSAGES, messagesReceivedCount);
    session.close();
    connection.close();
  }

}
