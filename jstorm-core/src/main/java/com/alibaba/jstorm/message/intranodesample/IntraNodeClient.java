package com.alibaba.jstorm.message.intranodesample;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;
import io.mappedbus.MappedBusWriter;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class IntraNodeClient implements IConnection {
    // 2 Longs for uuid, 1 int for total number of packets, and 1 int for packet number
    private static int metaDataExtent = 2*Long.BYTES + 2*Integer.BYTES;
    // 1 int for task#, 1 int for content.length, 1 int for componentID.length, 1 int for stream.length
    private static int constMsgExtent = 4*Integer.BYTES;
    private final String fileName;
    private final long fileSize;
    private final int packetSize;
    private final int packetDataSize;
    private int clientID;
    private MappedBusWriter writer;
    private ByteBuffer packet;
    private byte[] packetBytes;

    // TODO - test
    /*public static void main(String[] args) throws IOException {
        IntraNodeClient client = new IntraNodeClient(27, "test-bytearray", 2000000L, 64);
        for (int i = 0; i < 2; ++i) {
            byte[] content = ("I am " + i +" and time is " + new Date()).getBytes();
            String compId = "component " + i;
            String stream = "stream " + i +" ha ha";
            TaskMessage msg = new TaskMessage(i, content, compId, stream);
            client.write(msg);
        }
    }*/

    public IntraNodeClient(String fileName, long fileSize, int packetSize)
        throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        if (packetSize < metaDataExtent+constMsgExtent){
            throw new RuntimeException("Packet size (" + packetSize + ") should be greater or equal to " + (metaDataExtent+constMsgExtent) + "");
        }
        this.packetSize = packetSize;
        this.packetDataSize = packetSize - metaDataExtent;

        packetBytes = new byte[packetSize];
        this.packet = ByteBuffer.wrap(packetBytes);

        writer = new MappedBusWriter(fileName, fileSize, packetSize, false);
        writer.open();
    }

    private void write(TaskMessage msg) throws EOFException {
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
        offset+=Long.BYTES;
        packet.putLong(offset, uuid.getLeastSignificantBits());
        offset+=Long.BYTES;
        packet.putInt(offset, numPackets);
        offset+=Integer.BYTES;
        packet.putInt(offset, packetNumber);
        offset+=Integer.BYTES;
        packet.putInt(offset, msg.task());
        offset+=Integer.BYTES;
        packet.putInt(offset, content.length);
        offset+=Integer.BYTES;
        packet.putInt(offset, compIdLength);
        offset+=Integer.BYTES;
        packet.putInt(offset, streamLength);
        offset+=Integer.BYTES;

        int count = 0;
        while (count < content.length) {
            int remainingToWrite = content.length - count;
            int remainingCapacity = packetSize - offset;
            if (remainingCapacity == 0){
                writer.write(packetBytes, 0, packetSize);
                ++packetNumber;
                remainingCapacity = packetDataSize;
                offset = 2*Long.BYTES+Integer.BYTES;
                packet.putInt(offset, packetNumber);
                offset += Integer.BYTES;
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
                offset = 2*Long.BYTES+Integer.BYTES;
                packet.putInt(offset, packetNumber);
                offset += Integer.BYTES;
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
                offset = 2*Long.BYTES+Integer.BYTES;
                packet.putInt(offset, packetNumber);
                offset += Integer.BYTES;
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
                e.printStackTrace();
            }
        }
    }

    @Override
    public void send(TaskMessage message) {
        try {
            write(message);
        } catch (EOFException e) {
            e.printStackTrace();
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
}
