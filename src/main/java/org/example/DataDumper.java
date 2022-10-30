package org.example;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class DataDumper implements Runnable{
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<String>> map;
    private final String queueName;
    private final RMQChannelPool rmqChannelPool;
    public DataDumper(ConcurrentHashMap<String, CopyOnWriteArrayList<String>> map, String queueName, RMQChannelPool rmqChannelPool) {
        this.map = map;
        this.queueName = queueName;
        this.rmqChannelPool = rmqChannelPool;
    }

    @Override
    public void run() {

        try {
            Channel channel = rmqChannelPool.borrowObject();
            channel.queueDeclare(queueName, false, false, false, null);
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                String[] info = message.split("/");
                String skierID = info[7];
                map.putIfAbsent(skierID, new CopyOnWriteArrayList<>());
                map.get(skierID).add(message);
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                System.out.println(" [x] Received '" + message + "'");
            };
            channel.basicConsume(queueName, false, deliverCallback, consumerTag -> { });
            rmqChannelPool.returnObject(channel);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
}
