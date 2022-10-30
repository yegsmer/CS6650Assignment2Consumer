package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Consumer {

  private final static String QUEUE_NAME = "skiersQueue";
  private final static String RABBITMQ_URL = "172.31.23.23";
  private final static Integer POOL_SIZE = 400;

  private final static ConcurrentHashMap<String, CopyOnWriteArrayList<String>> map = new ConcurrentHashMap<>();

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(RABBITMQ_URL);
    Connection connection = factory.newConnection();
    RMQChannelFactory chanFactory = new RMQChannelFactory (connection);
    RMQChannelPool pool = new RMQChannelPool(POOL_SIZE, chanFactory);
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
    DataDumper dataDumper = new DataDumper(map, QUEUE_NAME, pool);
    for (int i = 0; i < POOL_SIZE; i++) {
      new Thread(dataDumper).start();
    }

  }
}
