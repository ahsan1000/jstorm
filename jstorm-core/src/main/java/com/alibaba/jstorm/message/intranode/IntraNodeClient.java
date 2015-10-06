package com.alibaba.jstorm.message.intranode;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;
import io.mappedbus.MappedBusWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class IntraNodeClient implements IConnection {
    private static Logger LOG = LoggerFactory.getLogger(IntraNodeServer.class);
    public static final int LONG_BYTES = 8;
    public static final int INTEGER_BYTES = 4;
    // 2 Longs for uuid, 1 int for total number of packets, and 1 int for packet number
    private static int metaDataExtent = 2*LONG_BYTES + 2*INTEGER_BYTES;
    // 1 int for task#, 1 int for content.length, 1 int for componentID.length, 1 int for stream.length
    private static int constMsgExtent = 4*INTEGER_BYTES;
    private final int packetSize;
    private final int packetDataSize;
    private MappedBusWriter writer;
    private ByteBuffer packet;
    private byte[] packetBytes;
    private String sharedFile;

    public IntraNodeClient(String baseFile, String supervisorId, int taskId, long fileSize, int packetSize)
        throws IOException {
        if (packetSize < metaDataExtent+constMsgExtent){
            throw new RuntimeException("Packet size (" + packetSize + ") should be greater or equal to " + (metaDataExtent+constMsgExtent) + "");
        }
        this.packetSize = packetSize;
        this.packetDataSize = packetSize - metaDataExtent;

        packetBytes = new byte[packetSize];
        this.packet = ByteBuffer.wrap(packetBytes);
        sharedFile = baseFile + "/" + supervisorId + "_" + taskId;
        LOG.info("Starting intrannode clien on: " + sharedFile);
        writer = new MappedBusWriter(sharedFile, fileSize, packetSize, false);
        writer.open();
    }

    private void write(TaskMessage msg) throws EOFException {
        LOG.info("Writing message: " + msg.task() + " " + msg.componentId() + ":" + msg.stream() + " to: " + sharedFile);
        UUID uuid = UUID.randomUUID();
        byte[] content = msg.message();
        // extent is metadata + msg
        // metadata is,
        // 2 long for uuid
        // 1 int for chunk number
        // msg is,
        // 1 int for task
        // 1 int for content length
        // content bytes
        // 1 int for msg.componentId string length
        // msg.componentId.length bytes
        // 1 int for msg.stream string length
        // msg.stream.lenght bytes

        final int
            compIdLength =
            msg.componentId()
                .length();
        final int
            streamLength =
            msg.stream()
                .length();
        int msgExtent = constMsgExtent + content.length + compIdLength + streamLength;

        int  numPackets = msgExtent / packetDataSize;
        int r = msgExtent % packetDataSize;
        if (r > 0) ++numPackets;

        int packetNumber = 0;
        int offset = 0;
        packet.putLong(offset, uuid.getMostSignificantBits());
        offset+=LONG_BYTES;
        packet.putLong(offset, uuid.getLeastSignificantBits());
        offset+=LONG_BYTES;
        packet.putInt(offset, numPackets);
        offset+=INTEGER_BYTES;
        packet.putInt(offset, packetNumber);
        offset+=INTEGER_BYTES;
        packet.putInt(offset, msg.task());
        offset+=INTEGER_BYTES;
        packet.putInt(offset, content.length);
        offset+=INTEGER_BYTES;
        packet.putInt(offset, compIdLength);
        offset+=INTEGER_BYTES;
        packet.putInt(offset, streamLength);
        offset+=INTEGER_BYTES;

        int count = 0;
        while (count < content.length) {
            int remainingToWrite = content.length - count;
            int remainingCapacity = packetSize - offset;
            if (remainingCapacity == 0){
                writer.write(packetBytes, 0, packetSize);
                ++packetNumber;
                remainingCapacity = packetDataSize;
                offset = 2*LONG_BYTES+INTEGER_BYTES;
                packet.putInt(offset, packetNumber);
                offset += INTEGER_BYTES;
            }
            int willWrite = Math.min(remainingCapacity, remainingToWrite);
            packet.position(offset);
            packet.put(content, count, willWrite);
            count+=willWrite;
            offset+=willWrite;
        }

        final byte[]
            compIdBytes =
            msg.componentId()
                .getBytes();
        count = 0;
        while (count < compIdLength) {
            int remainingToWrite = compIdLength - count;
            int remainingCapacity = packetSize - offset;
            if (remainingCapacity == 0){
                writer.write(packetBytes, 0, packetSize);
                ++packetNumber;
                remainingCapacity = packetDataSize;
                offset = 2*LONG_BYTES+INTEGER_BYTES;
                packet.putInt(offset, packetNumber);
                offset += INTEGER_BYTES;
            }
            int willWrite = Math.min(remainingCapacity, remainingToWrite);
            packet.position(offset);
            packet.put(compIdBytes, count, willWrite);
            count+=willWrite;
            offset+=willWrite;
        }

        final byte[]
            streamBytes =
            msg.stream()
                .getBytes();
        count = 0;
        while (count < streamLength) {
            int remainingToWrite = streamLength - count;
            int remainingCapacity = packetSize - offset;
            if (remainingCapacity == 0){
                writer.write(packetBytes, 0, packetSize);
                ++packetNumber;
                remainingCapacity = packetDataSize;
                offset = 2*LONG_BYTES+INTEGER_BYTES;
                packet.putInt(offset, packetNumber);
                offset += INTEGER_BYTES;
            }
            int willWrite = Math.min(remainingCapacity, remainingToWrite);
            packet.position(offset);
            packet.put(streamBytes, count, willWrite);
            count+=willWrite;
            offset+=willWrite;
        }

        writer.write(packetBytes, 0, packetSize);

    }

    @Override
    public Object recv(Integer taskId, int flags) {
        throw new UnsupportedOperationException(
                "recvTask: Client connection should not receive any messages");
    }

    @Override
    public void registerQueue(Integer taskId, DisruptorQueue recvQueu) {
        throw new UnsupportedOperationException(
                "recvTask: Client connection should not receive any messages");
    }

    @Override
    public void enqueue(TaskMessage message) {
        throw new UnsupportedOperationException(
                "recvTask: Client connection should not receive any messages");
    }

    @Override
    public void send(List<TaskMessage> messages) {
        for (TaskMessage taskMessage : messages) {
            try {
                write(taskMessage);
            } catch (EOFException e) {
                throw new RuntimeException("Faile to send message", e);
            }
        }
    }

    @Override
    public void send(TaskMessage message) {
        try {
            write(message);
        } catch (EOFException e) {
            throw new RuntimeException("Failed to send message", e);
        }
    }

    @Override
    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    public static void main(String[] args) {
//        String baseFile = "/home/supun/dev/projects/jstorm-modified";
        String baseFile = "/dev/shm";
        String nodeFile = "nodeFile";
        IntraNodeServer server = new IntraNodeServer(baseFile, nodeFile, 1, IntraNodeServer.DEFAULT_FILE_SIZE, new ConcurrentHashMap<Integer, DisruptorQueue>());
        try {
            IntraNodeClient client = new IntraNodeClient(baseFile, nodeFile, 1, IntraNodeServer.DEFAULT_FILE_SIZE, IntraNodeServer.PACKET_SIZE);
            for (int i = 0; i < 100; i++) {
                client.send(new TaskMessage(1, "Hello".getBytes(), "1", "2"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
