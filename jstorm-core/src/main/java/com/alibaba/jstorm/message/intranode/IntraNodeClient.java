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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
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

    public IntraNodeClient(String baseFile, String supervisorId, int targetTaskId, int sourceTaskId, long fileSize, int packetSize)
        throws IOException {
        if (packetSize < metaDataExtent+constMsgExtent){
            throw new RuntimeException("Packet size (" + packetSize + ") should be greater or equal to " + (metaDataExtent+constMsgExtent) + "");
        }
        this.packetSize = packetSize;
        this.packetDataSize = packetSize - metaDataExtent;

        packetBytes = new byte[packetSize];
        this.packet = ByteBuffer.wrap(packetBytes);
        sharedFile = baseFile + "/" + supervisorId + "_" +  targetTaskId + "_" + sourceTaskId;
        LOG.info("Starting intrannode clien on: " + sharedFile);
        writer = new MappedBusWriter(sharedFile, fileSize, packetSize, false);
        writer.open();
    }
    int totalPacketCount = 0;
    private synchronized void write(TaskMessage msg) throws EOFException {
        //LOG.info("Writing message: " + msg.task() + " " + msg.sourceTask() + ":" + msg.stream() + " to: " + sharedFile);
        UUID uuid = UUID.randomUUID();
        byte[] content = msg.message();
        // extent is metadata + msg
        // metadata is,
        // 2 long for uuid
        // 1 int for total packets
        // 1 int for packet number
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
                (msg.sourceTask() + "")
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
        int packetCount = 0;
        int count = 0;
        while (count < content.length) {
            int remainingToWrite = content.length - count;
            int remainingCapacity = packetSize - offset;
            if (remainingCapacity == 0){
                packetCount++;
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
                (msg.sourceTask() + "")
                .getBytes();
        count = 0;
        while (count < compIdLength) {
            int remainingToWrite = compIdLength - count;
            int remainingCapacity = packetSize - offset;
            if (remainingCapacity == 0){
                packetCount++;
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
                packetCount++;
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
        packetCount++;
        writer.write(packetBytes, 0, packetSize);
//        if (packetCount != 2) {
//            System.out.println("*************************************************************************************************");
//        }
        totalPacketCount += packetCount;
        LOG.info("Writing message: " + msg.task() + " " + msg.sourceTask() + ":" + msg.stream() + " with packets:" + numPackets +" to: " + sharedFile);

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
            } catch (Throwable e) {
                String message = "Faild to send message";
                LOG.error(message);
                throw new RuntimeException(message, e);
            }
        }
    }

    @Override
    public void send(TaskMessage message) {
        try {
            write(message);
        } catch (Throwable e) {
            String msg = "Faild to send message";
            LOG.error(msg);
            throw new RuntimeException(msg, e);
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
//        String baseFile = "E:\\";
        String baseFile = "/dev/shm";
//        String baseFile = "/home/supun/dev/projects/jstorm-modified";
        String nodeFile = "nodeFile";
//        try {
//            Files.deleteIfExists(Paths.get(baseFile + "/" + nodeFile + "_" + 1));
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }
//        IntraNodeServer server = new IntraNodeServer(baseFile, nodeFile, 1, 1, IntraNodeServer.DEFAULT_FILE_SIZE, new ConcurrentHashMap<Integer, DisruptorQueue>());

        try {
            final IntraNodeClient client = new IntraNodeClient(baseFile, nodeFile, 1, 1, IntraNodeServer.DEFAULT_FILE_SIZE, IntraNodeServer.PACKET_SIZE);
            String s = "1";
            Random random = new Random();
            byte b[] = new byte[1000];
            random.nextBytes(b);
            for (int j = 0; j < 1; j++) {
                final String finalS = s;
                Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        for (int i = 0; i < 1000000; i++) {
                            client.send(new TaskMessage(1, finalS.getBytes(), 1, "" + i));
                        }
                    }
                });
                t.start();
            }
            System.out.println("******************************************   total packet count: " + client.totalPacketCount);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
