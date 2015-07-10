package org.nlab.article.fairconsuming;

import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;


public class DwrrBlockingQueueConsumer {

    public static final Logger LOG = LoggerFactory.getLogger(DwrrBlockingQueueConsumer.class);

    private final BlockingDeque<QueueingConsumer.Delivery> deliveries;
    private final String queueName;
    private final Channel channel;
    private final Semaphore consumerToken;
    private final InternalConsumer consumer;


    public DwrrBlockingQueueConsumer(String queueName, Channel channel, Semaphore consumerToken, int prefetch) {
        this.queueName = queueName;
        this.channel = channel;
        this.consumerToken = consumerToken;
        this.deliveries = new LinkedBlockingDeque<>(prefetch);
        this.consumer = new InternalConsumer(channel);
    }

    /**
     * Start consuming messages from the queue
     *
     * @throws IOException
     */
    public void start() throws IOException {
        //Subscribe to the queue and enable the acknowledgement
        channel.basicConsume(queueName, false, consumer);
    }

    /**
     * Stop consuming messages from the queue
     *
     * @throws IOException
     */
    public void stop() throws IOException {
        channel.basicCancel(consumer.getConsumerTag());
    }


    public BlockingDeque<QueueingConsumer.Delivery> getDeliveries() {
        return deliveries;
    }

    private class InternalConsumer extends DefaultConsumer {
        public InternalConsumer(Channel channel) {
            super(channel);
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
            if (!deliveries.offer(new QueueingConsumer.Delivery(envelope, properties, body))){
                LOG.error("Cannot handle this new message, must not happen as the prefetch is configured");
            }
            //Release a consumer token
            consumerToken.release();
        }
    }

    public String getQueueName() {
        return queueName;
    }

    public Channel getChannel() {
        return channel;
    }
}
