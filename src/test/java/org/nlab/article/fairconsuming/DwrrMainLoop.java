package org.nlab.article.fairconsuming;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

/**
 * Created by nithril on 26/10/14.
 */
public class DwrrMainLoop implements Runnable {

    public static final Logger LOG = LoggerFactory.getLogger(DwrrMainLoop.class);

    public static final int MESSAGE_WEIGHT = 4;

    private final List<DwrrSlot> slots;
    private final Semaphore consumerToken;
    private final Channel channel;
    private final Consumer<QueueingConsumer.Delivery> consumer;

    private volatile boolean stopProcessing;

    public DwrrMainLoop(List<DwrrSlot> slots, Semaphore consumerToken, Channel channel, Consumer<QueueingConsumer.Delivery> consumer) {
        this.slots = slots;
        this.consumerToken = consumerToken;
        this.channel = channel;
        this.consumer = consumer;
    }

    public void run() {

        try {

            while (!stopProcessing) {
                int processedMessageCounter = 0;

                //Try to acquire a consumer token, wait for 5 seconds
                if (consumerToken.tryAcquire(1, 5, TimeUnit.SECONDS)) {
                    //We got a token, a delivery is available
                    for (DwrrSlot slot : slots) {
                        //The deficit is reduced per iteration
                        slot.reduceDeficit();

                        //Loop until the slot does not contain any deliveries or until the slot deficit is bellow the message weight
                        while (!slot.getConsumer().getDeliveries().isEmpty() && slot.getDeficit() >= MESSAGE_WEIGHT) {
                            QueueingConsumer.Delivery delivery = slot.getConsumer().getDeliveries().poll();
                            slot.increaseDeficit(MESSAGE_WEIGHT);
                            //Consume
                            consumer.accept(delivery);
                            //Finally ack the message, so RabbitMQ will push a new one to this slot
                            slot.getConsumer().getChannel().basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                        }
                        //If the slot does not contain any deliveries, reset the deficit
                        if (slot.getConsumer().getDeliveries().isEmpty()) {
                            slot.reset();
                        }
                    }

                    if (processedMessageCounter > 0) {
                        //If we have processed message, we must acquire the number of processed message minus the first acquire
                        consumerToken.acquire(processedMessageCounter - 1);
                    } else {
                        //If we do not have processed message (because all deficit where < weight) we must release the token
                        consumerToken.release();
                    }

                } else {
                    LOG.info("No message");
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public void stop() {
        stopProcessing = true;
    }
}
