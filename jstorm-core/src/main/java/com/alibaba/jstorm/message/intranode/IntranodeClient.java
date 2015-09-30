package com.alibaba.jstorm.message.intranode;


import backtype.storm.messaging.TaskMessage;

public class IntranodeClient {
    private int clientID;

    public IntranodeClient(int clientID, String) {
        this.clientID = clientID;
    }

    public void write(TaskMessage){

    }
}
