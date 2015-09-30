package com.alibaba.jstorm.message.internode;


import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;

import java.util.List;

public class InterNodeClient implements IConnection {

    @Override
    public Object recv(Integer taskId, int flags) {
        return null;
    }

    @Override
    public void registerQueue(Integer taskId, DisruptorQueue recvQueu) {

    }

    @Override
    public void enqueue(TaskMessage message) {

    }

    @Override
    public void send(List<TaskMessage> messages) {

    }

    @Override
    public void send(TaskMessage message) {

    }

    @Override
    public void close() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
