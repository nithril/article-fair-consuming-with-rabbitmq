package org.nlab.article.fairconsuming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;


public class TestDwrr {

    public static final Logger LOG = LoggerFactory.getLogger(TestDwrr.class);

    private static final int NB_QUEUE = 10;
    private static final int NB_MESSAGES_PER_QUEUE = 40000;
    private static final int PREFETCH = 50;

    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;


    private List<DwrrSlot> slots = new ArrayList<>(NB_QUEUE);

    private Semaphore consumerToken = new Semaphore(0);

    private ExecutorService executorService = Executors.newFixedThreadPool(20);

    @Before
    public void init() throws IOException, TimeoutException {
        factory = new ConnectionFactory();

        factory.setHost("localhost");
        factory.setVirtualHost("fair");
        factory.setUsername("fair");
        factory.setPassword("password");
        factory.setSharedExecutor(executorService);

        connection = factory.newConnection();

        channel = connection.createChannel();
        channel.basicQos(PREFETCH, false);

        for (int queueIndex = 0; queueIndex < NB_QUEUE; queueIndex++) {
            String queueName = buildQueueName(queueIndex);
            channel.queueDeclare(queueName, false, false, true, null);
            DwrrBlockingQueueConsumer consumer = new DwrrBlockingQueueConsumer(queueName, channel, consumerToken, PREFETCH);
            slots.add(new DwrrSlot(queueName, consumer, (queueIndex + 1) * DwrrMainLoop.MESSAGE_WEIGHT));
        }
    }


    @Test
    public void test() throws IOException, InterruptedException {

        LOG.info("Publishing Message....");
        byte[] payload = new byte[]{0, 1, 2, 3};

        AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder().deliveryMode(1).build();
        for (int i = 0; i < NB_MESSAGES_PER_QUEUE * NB_QUEUE; i++) {
            channel.basicPublish("", buildQueueName(i % NB_QUEUE), basicProperties, payload);
        }


        LOG.info("Consuming Message....");

        //We will wait until NB_MESSAGES_PER_QUEUE * NB_QUEUE / 2 messages are consumed
        //At the end of the test, queues must contain messages in order to output correct results
        final CountDownLatch countDownLatch = new CountDownLatch(NB_MESSAGES_PER_QUEUE * NB_QUEUE / 2);

        //Create the main loop instance
        DwrrMainLoop dwrrMainLoop = new DwrrMainLoop(slots, consumerToken, channel, d -> {
            countDownLatch.countDown();
            waitFor(100000);
        });

        //Start the consumers
        for (DwrrSlot slot : slots) {
            slot.getConsumer().start();
        }
        long start = System.currentTimeMillis();

        executorService.execute(dwrrMainLoop);

        countDownLatch.await(100, TimeUnit.SECONDS);

        long end = System.currentTimeMillis();

        dwrrMainLoop.stop();
        for (DwrrSlot slot : slots) {
            slot.getConsumer().stop();
            LOG.info("Name={} Quantum={} Consumed msg={} Msg/s={}", slot.getQueueName(), slot.getQuantum(), slot.getProcessedMsg(), (1000f * slot.getProcessedMsg() / (end - start)));
        }
    }


    private String buildQueueName(int i) {
        return "p" + i;
    }


    private void waitFor(int interval) {
        long start = System.nanoTime();
        while (start + interval > System.nanoTime()) ;
    }
}
