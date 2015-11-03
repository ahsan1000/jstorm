package com.alibaba.jstorm.message.intranode;

import backtype.storm.messaging.IConnection;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IntraNodeClients {
    private static Logger LOG = LoggerFactory.getLogger(IntraNodeClients.class);
    private final int packetSize;
    private String sharedFile;
    private Chronicle outbound;

    public IntraNodeClients(String baseFile, String supervisorId, int targetTaskId, int sourceTaskId, int packetSize) {
        this.packetSize = packetSize;
        sharedFile = baseFile + "/" + supervisorId + "_" +  targetTaskId;

        try {
            outbound = ChronicleQueueBuilder
                    .vanilla(sharedFile).synchronous(false)
                    .build();
        } catch (IOException e) {
            String s = "Failed to create the memory mapped queue: " + sharedFile;
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    private Map<Integer, IntraNodeClient> clients = new HashMap<Integer, IntraNodeClient>();

    public synchronized IConnection getClient(int taskId) {
        IntraNodeClient client = clients.get(taskId);
        if (client == null) {
            try {
                ExcerptAppender writer = outbound.createAppender();
                client = new IntraNodeClient(sharedFile, packetSize, writer);
                clients.put(taskId, client);
            } catch (IOException e) {
                String s = "Failed to create appender for file: " + sharedFile;
                LOG.error(s, e);
                throw new RuntimeException(s, e);
            }
        }
        return client;
    }
}
