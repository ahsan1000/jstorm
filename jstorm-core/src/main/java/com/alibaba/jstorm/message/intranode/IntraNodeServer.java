package com.alibaba.jstorm.message.intranode;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;
import io.mappedbus.MappedBusReader;
import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.VanillaChronicle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class IntraNodeServer implements IConnection {
    private static Logger LOG = LoggerFactory.getLogger(IntraNodeServer.class);
    public static final int LONG_BYTES = 8;
    public static final int INTEGER_BYTES = 4;
    public static final int PACKET_SIZE = 1024;

    private HashMap<UUID, ArrayList<ByteBuffer>> msgs = new HashMap<UUID, ArrayList<ByteBuffer>>();
    private ConcurrentHashMap<Integer, DisruptorQueue> deserializeQueues;

    private boolean run = true;

    private int count = 0, count2 = 0;
    private String sharedFile;

    private ExcerptTailer tailer;
    private Thread serverThread;

    public IntraNodeServer(String baseFile, String supervisorId, int sourceTask, int targetTask, ConcurrentHashMap<Integer, DisruptorQueue> deserializeQueues) {
        this.deserializeQueues = deserializeQueues;
        sharedFile = baseFile + "/" + supervisorId + "_" + sourceTask;

        try {
            Chronicle inbound = ChronicleQueueBuilder
                    .vanilla(sharedFile).cycle(VanillaChronicle.Cycle.SECONDS).cycleLength(3600000)
                    .build();
            tailer = inbound.createTailer().toEnd();
            serverThread = new Thread(new ServerWorker());
            serverThread.start();
        } catch (IOException e) {
            String s = "Failed to create the memory mapped queue";
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }
    }

    public class ServerWorker implements Runnable {

        public void run() {
            int packetSize = PACKET_SIZE;
            byte[] bytes = new byte[packetSize];
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            int length, totalPackets;
            UUID uuid;
            ArrayList<ByteBuffer> packets;
            boolean isFresh;

            while (run) {
                if (tailer.nextIndex()) {
                    length = tailer.read(bytes);
                    assert length == packetSize;
                    uuid = new UUID(buffer.getLong(0),
                            buffer.getLong(LONG_BYTES));
                    totalPackets = buffer.getInt(2 * LONG_BYTES);
                    packets = msgs.get(uuid);
                    if ((isFresh = packets == null)){
                        packets = new ArrayList<ByteBuffer>();
                    }
                    packets.add(ByteBuffer.wrap(Arrays.copyOf(bytes,
                            bytes.length)));

                    count2++;
                    //LOG.info("Received memory message with packets {}, to worker {}", totalPackets, sharedFile);
                    if (packets.size() == totalPackets){
                        createMsg(isFresh ? packets : msgs.remove(uuid));
                        continue;
                    }
                    if (isFresh){
                        msgs.put(uuid, packets);
                    }
                }
            }
        }
    }

    private void createMsg(ArrayList<ByteBuffer> packets) {
        Collections.sort(packets, new Comparator<ByteBuffer>() {
            public int compare(ByteBuffer p1, ByteBuffer p2) {
                int offset = 2 * LONG_BYTES;
                final int packetNum1 = p1.getInt(offset);
                final int packetNum2 = p2.getInt(offset);
                return packetNum1 < packetNum2 ? -1 : (packetNum1 == packetNum2 ? 0 : 1);
            }
        });
        int packetNumber = 0;
        ByteBuffer packet = packets.get(packetNumber);
        int packetSize = packet.capacity();
        int metaDataExtent = 2 * LONG_BYTES + 2 * INTEGER_BYTES;
        int packetDataSize = packet.capacity() - metaDataExtent;
        int offset = 2*LONG_BYTES+2*INTEGER_BYTES;
        int task = packet.getInt(offset);
        offset += INTEGER_BYTES;
        int contentLength = packet.getInt(offset);
        offset += INTEGER_BYTES;
        int compIdLength = packet.getInt(offset);
        offset += INTEGER_BYTES;
        int streamLength = packet.getInt(offset);
        offset += INTEGER_BYTES;

        byte[] content = new byte[contentLength];
        byte[] compId = new byte[compIdLength];
        byte[] stream = new byte[streamLength];

        int count = 0;
        while (count < contentLength) {
            int remainingToRead = contentLength - count;
            int remainingCapacity = packetSize - offset;
            if (remainingCapacity == 0){
                ++packetNumber;
                remainingCapacity = packetDataSize;
                offset = 2*LONG_BYTES+2*INTEGER_BYTES;
                packet = packets.get(packetNumber);
            }
            int willRead = Math.min(remainingCapacity, remainingToRead);
            packet.position(offset);
            packet.get(content, count, willRead);
            count+=willRead;
            offset+=willRead;
        }

        count = 0;
        while (count < compIdLength) {
            int remainingToRead = compIdLength - count;
            int remainingCapacity = packetSize - offset;
            if (remainingCapacity == 0){
                ++packetNumber;
                remainingCapacity = packetDataSize;
                offset = 2*LONG_BYTES+2*INTEGER_BYTES;
                packet = packets.get(packetNumber);
            }
            int willRead = Math.min(remainingCapacity, remainingToRead);
            packet.position(offset);
            packet.get(compId, count, willRead);
            count+=willRead;
            offset+=willRead;
        }

        count = 0;
        while (count < streamLength) {
            int remainingToRead = streamLength - count;
            int remainingCapacity = packetSize - offset;
            if (remainingCapacity == 0){
                ++packetNumber;
                remainingCapacity = packetDataSize;
                offset = 2*LONG_BYTES+2*INTEGER_BYTES;
                packet = packets.get(packetNumber);
            }
            int willRead = Math.min(remainingCapacity, remainingToRead);
            packet.position(offset);
            packet.get(stream, count, willRead);
            count+=willRead;
            offset+=willRead;
        }

        TaskMessage msg = new TaskMessage(task, content, Integer.parseInt(new String(compId)), new String(stream));
        String msgStream = msg.stream();
        // LOG.info("Recvd message: " + msg.task() + " " + Integer.parseInt(new String(compId)) + ":" + msgStream + ": count: " + ++this.count);
        enqueue(msg);
    }

    @Override
    public Object recv(Integer taskId, int flags) {
        return null;
    }

    @Override
    public void registerQueue(Integer taskId, DisruptorQueue recvQueu) {
        deserializeQueues.put(taskId, recvQueu);
    }

    @Override
    public void enqueue(TaskMessage message) {
        int task = message.task();

        DisruptorQueue queue = deserializeQueues.get(task);
        if (queue == null) {
            LOG.debug("Received invalid message directed at port " + task
                    + ". Dropping...");
            return;
        }

        queue.publish(message);
    }

    @Override
    public void send(List<TaskMessage> messages) {
        throw new UnsupportedOperationException(
                "Server connection should not send any messages");
    }

    @Override
    public void send(TaskMessage message) {
        throw new UnsupportedOperationException(
                "Server connection should not send any messages");
    }

    @Override
    public void close() {
        try {
            run = false;
            if (serverThread != null) {
                serverThread.join();
            }
            tailer.close();
        } catch (InterruptedException ignore) {
        }
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    public static void main(String[] args) {
        String baseFile = "/dev/shm";
//        String baseFile = "";
        String nodeFile = "ec10a060-7950-440b-88f0-d09baf3bc863";
//        try {
//            Files.deleteIfExists(Paths.get(baseFile + "/" + nodeFile + "_" + 1));
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }
        IntraNodeServer server = new IntraNodeServer(baseFile, nodeFile, 6801, 6802, new ConcurrentHashMap<Integer, DisruptorQueue>());
    }
}

