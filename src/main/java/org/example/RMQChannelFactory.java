/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.example;

/**
 *
 * @author Ian Gorton, Northeastern University
 * The examples supplement Chapter 7 of the Foundations of Scalable Systems, O'Reilly Media 2022
 */


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.io.IOException;

/**
 * A simple RabbitMQ channel factory based on the APache pooling libraries
 */
public class RMQChannelFactory extends BasePooledObjectFactory<Channel> {

  private final Connection connection;
  private int count;

  public RMQChannelFactory(Connection connection) {
    this.connection = connection;
    count = 0;
  }

  @Override
  synchronized public Channel create() throws IOException {
    count ++;
    Channel chan = connection.createChannel();
    return chan;

  }

  @Override
  public PooledObject<Channel> wrap(Channel channel) {
    return new DefaultPooledObject<>(channel);
  }

  public int getChannelCount() {
    return count;
  }
}