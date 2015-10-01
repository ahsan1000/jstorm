package com.alibaba.jstorm.message.intranodesample;

import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.utils.DisruptorQueue;
import io.mappedbus.MappedBusReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class IntraNodeServer implements IConnection {
    private static Logger LOG = LoggerFactory.getLogger(IntraNodeServer.class);

    // 2 Longs for uuid, 1 int for total number of packets, and 1 int for packet number
    private static int metaDataExtent = 2*Long.BYTES + 2*Integer.BYTES;
    private HashMap<UUID, ArrayList<ByteBuffer>> msgs = new HashMap<UUID, ArrayList<ByteBuffer>>();
    private ConcurrentHashMap<Integer, DisruptorQueue> deserializeQueues;

    private MappedBusReader reader;

    public void run() {
        try {
            final int packetSize = 64;
            this.reader = new MappedBusReader("test-bytearray", 2000000L, packetSize);
            reader.open();

            byte[] bytes = new byte[packetSize];
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            while (true) {
                if (reader.next()) {
                    int length = reader.readBuffer(bytes, 0);
                    assert length == packetSize;
                    final UUID uuid = new UUID(buffer.getLong(0),
                                               buffer.getLong(Long.BYTES));
                    // TODO - test
                    /*final String tmp = "numPackets:" + buffer.getInt(2 * Long.BYTES)
                                     + "packetNumber:" + buffer.getInt(
                        2 * Long.BYTES + Integer.BYTES);
                    System.out.println(tmp);*/
                    if (msgs.containsKey(uuid)){
                        final ArrayList<ByteBuffer> packets = msgs.get(uuid);
                        packets.add(ByteBuffer.wrap(Arrays.copyOf(bytes,
                                                                  bytes.length)));
                        if (packets.size() == buffer.getInt(2*Long.BYTES)){
                            createMsg(msgs.remove(uuid));
                        }
                    } else{

                        final ArrayList<ByteBuffer> packets = new ArrayList<>();
                        packets.add(ByteBuffer.wrap(Arrays.copyOf(bytes,
                                                                  bytes.length)));
                        msgs.put(uuid, packets);
                    }
                }
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createMsg(ArrayList<ByteBuffer> packets) {
        Collections.sort(packets, new Comparator<ByteBuffer>() {
            @Override
            public int compare(ByteBuffer p1, ByteBuffer p2) {
                int offset = 2*Long.BYTES;
                final int packetNum1 = p1.getInt(offset);
                final int packetNum2 = p2.getInt(offset);
                return packetNum1 < packetNum2 ? -1 : (packetNum1 == packetNum2 ? 0 : 1);
            }
        });
        int packetNumber = 0;
        ByteBuffer packet = packets.get(packetNumber);
        int packetSize = packet.capacity();
        int packetDataSize = packet.capacity() - metaDataExtent;
        int offset = 2*Long.BYTES+2*Integer.BYTES;
        int task = packet.getInt(offset);
        offset += Integer.BYTES;
        int contentLength = packet.getInt(offset);
        offset += Integer.BYTES;
        int compIdLength = packet.getInt(offset);
        offset += Integer.BYTES;
        int streamLength = packet.getInt(offset);
        offset += Integer.BYTES;

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
                offset = 2*Long.BYTES+2*Integer.BYTES;
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
                offset = 2*Long.BYTES+2*Integer.BYTES;
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
                offset = 2*Long.BYTES+2*Integer.BYTES;
                packet = packets.get(packetNumber);
            }
            int willRead = Math.min(remainingCapacity, remainingToRead);
            packet.position(offset);
            packet.get(stream, count, willRead);
            count+=willRead;
            offset+=willRead;
        }

        TaskMessage msg = new TaskMessage(task, content, new String(compId), new String(stream));
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
            reader.close();
        } catch (IOException e) {
            LOG.warn("Failed to close reader", e);
        }
    }

    @Override
    public boolean isClosed() {
        return false;
    }
}

