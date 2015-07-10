package org.nlab.article.fairconsuming;

/**
 * Created by nlabrot on 05/07/15.
 */
public class DwrrSlot {

    private final int quantum;
    private int deficit;
    private String queueName;
    private final DwrrBlockingQueueConsumer consumer;
    private int processedMsg;

    public DwrrSlot(String queueName, DwrrBlockingQueueConsumer consumer, int quantum) {
        this.queueName = queueName;
        this.consumer = consumer;
        this.quantum = quantum;
    }

    public void reset(){
        deficit = 0;
    }

    public int reduceDeficit(){
        deficit = deficit + quantum;
        return deficit;
    }

    public int increaseDeficit(int cost){
        deficit = deficit - cost;
        processedMsg = processedMsg + 1;
        return deficit;
    }

    public DwrrBlockingQueueConsumer getConsumer() {
        return consumer;
    }

    public int getDeficit() {
        return deficit;
    }

    public int getQuantum() {
        return quantum;
    }

    public int getProcessedMsg() {
        return processedMsg;
    }

    public String getQueueName() {
        return queueName;
    }
}

