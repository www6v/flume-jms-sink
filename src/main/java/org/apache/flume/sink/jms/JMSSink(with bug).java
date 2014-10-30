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

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSink;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class JMSSink extends AbstractSink implements PollableSink, Configurable {

  private static final Logger logger = LoggerFactory.getLogger(JMSSink.class);

  private static final String JMS_TOPIC = "topic";
  private static final String JMS_QUEUE = "queue";

  private javax.naming.Context jndiContext = null;

  private ConnectionFactory jmsConnectionFactory = null;
  private Connection jmsConnection = null;
  private Session jmsSession = null;
  private Destination jmsDestination = null;
  private MessageProducer jmsProducer = null;

  private CounterGroup counterGroup;

  private String initialContextFactory;
  private String brokerUrl;
  private String destinationName;
  private String destinationType;

  public JMSSink() {
    counterGroup = new CounterGroup();
  }

  public void configure(Context context) {

    initialContextFactory = context.get("initialcontextfactory",
        String.class);
    brokerUrl = context.get("brokerurl", String.class);
    destinationName = context.get("destination", String.class);
    destinationType = context.get("destinationtype", String.class);

    Preconditions.checkState(initialContextFactory != null,
        "No initial context factory specified");
    Preconditions.checkState(brokerUrl != null, "No broker url specified");
    Preconditions.checkState(destinationName != null,
        "No destination specified");
    Preconditions.checkState(destinationType != null,
        "No destination type specified");
    Preconditions.checkState(destinationType.equalsIgnoreCase(JMS_TOPIC)
        || destinationType.equalsIgnoreCase(JMS_QUEUE),
        "destinationtype can only be either '" + JMS_QUEUE + "' or '"
            + JMS_TOPIC + "'");

    this.logger.info("JMS sink initialized");
  }

  private void createConnection() throws IOException {

    if (jndiContext == null) {
      try {
        Properties contextProperties = new Properties();
        contextProperties.setProperty(
            javax.naming.Context.INITIAL_CONTEXT_FACTORY,
            initialContextFactory);
        contextProperties.setProperty(
            javax.naming.Context.PROVIDER_URL, brokerUrl);

        jndiContext = new InitialContext(contextProperties);
      } catch (NamingException exception) {
        logger.error("Could not create JNDI context: "
            + exception.toString());
        return;
      }
    }

    if (jmsConnectionFactory == null) {
      try {
        jmsConnectionFactory = (ConnectionFactory) jndiContext
            .lookup("ConnectionFactory");
      } catch (NamingException e) {
        logger.error("Could not lookup ConnectionFactory: " + e);
        return;
      }
    }

    try {
      // XXX Handle user/password
      jmsConnection = jmsConnectionFactory.createConnection();
    } catch (JMSException e) {
      logger.error("Could not create connection to broker: " + e);
    }

    try {
      // XXX Makes it configurable
      jmsSession = jmsConnection.createSession(true,
          Session.SESSION_TRANSACTED);
    } catch (JMSException e) {
      logger.error("Could not create session: " + e);
    }

    try {
      jmsProducer = jmsSession.createProducer(jmsDestination);
    } catch (JMSException e) {
      logger.error("Could not create producer: " + e);
    }

    try {
      if (destinationType.equalsIgnoreCase(JMS_QUEUE)) {
        jmsDestination = jmsSession.createQueue(destinationName);
      } else {
        // If it's not a queue, it is a topic
        jmsDestination = jmsSession.createTopic(destinationName);
      }
    } catch (JMSException e) {
      logger.error("Could not create destination" + destinationName
          + ": " + e);
      return;
    }
  }

  private void destroyConnection() {
    try {
      if (jmsSession != null) {
        jmsSession.close();
      }
      
      if (jmsConnection != null) {
        jmsConnection.close();
      }
    } catch (JMSException e) {
      logger.error("Could destroy connection: " + e);
      return;
    }
  }

  @Override
  public void start() {
    logger.info("JMS sink {} starting", this.getName());

    try {
      createConnection();
    } catch (Exception e) {
      logger.error("Unable to create jms sink " + this.getName() + 
          " to broker" + brokerUrl + ". Exception follows.", e);

      /* Try to prevent leaking resources. */
      destroyConnection();

      /* FIXME: Mark ourselves as failed. */
      return;
    }

    super.start();

    logger.debug("JMS sink {} started", this.getName());
  }

  @Override
  public void stop() {
    logger.info("JMS sink {} stopping", this.getName());

    destroyConnection();

    super.stop();

    logger.debug("JMS sink {} stopped. Metrics:{}", this.getName(), counterGroup);
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();
      createConnection();

      Event event = channel.take();

      if (event == null) {
        counterGroup.incrementAndGet("event.empty");
        status = Status.BACKOFF;
      } else {
        sendEvent(event);
        counterGroup.incrementAndGet("event.jms");
      }
      
      jmsSession.commit();
      transaction.commit();

    } catch (ChannelException e) {
      try {
      if (jmsSession != null) {
          jmsSession.rollback();
      }
      } catch (JMSException e1) {
        logger.error("Unable to rollback JMS transaction. Exception follows.", e);
      }
      transaction.rollback();
      logger.error(
          "Unable to get event from channel. Exception follows.", e);
      status = Status.BACKOFF;

    } catch (Exception e) {
      try {
      if (jmsSession != null) {
        jmsSession.rollback();
      }
      } catch (JMSException e1) {
        logger.error("Unable to rollback JMS transaction. Exception follows.", e);
      }
      transaction.rollback();
      logger.error(
          "Could not complete transaction. Exception follows.",
          e);
      status = Status.BACKOFF;
      destroyConnection();

    } finally {
      transaction.close();
    }

    return status;
  }

  private void sendEvent(Event event) throws JMSException {

    BytesMessage message = jmsSession.createBytesMessage();
    
    Map<String, String> headers = event.getHeaders();
    for(Entry<String, String> entry: headers.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      
      message.setStringProperty(key, value);
    }
    
    message.writeBytes(event.getBody());
    jmsProducer.send(message);
  }
}
