/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.jstorm.task;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.messaging.IConnection;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.tuple.TupleExt;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.callback.AsyncLoopRunnable;
import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.common.metric.MetricRegistry;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.common.metric.Timer;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.metric.JStormHealthCheck;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Sending entrance
 * 
 * Task sending all tuples through this Object
 * 
 * Serialize the Tuple and put the serialized data to the sending queue
 * 
 * @author yannian
 * 
 */
public class TaskTransfer {

    private static Logger LOG = LoggerFactory.getLogger(TaskTransfer.class);

    protected Map storm_conf;
    protected DisruptorQueue transferQueue;
    protected KryoTupleSerializer serializer;
    protected Map<Integer, DisruptorQueue> innerTaskTransfer;
    protected DisruptorQueue serializeQueue;
    protected final AsyncLoopThread serializeThread;
    protected volatile TaskStatus taskStatus;
    protected String taskName;
    protected Timer timer;
    protected Task task;
    protected int taskId;
    
    protected ConcurrentHashMap<WorkerSlot, IConnection> nodeportSocket;
    protected ConcurrentHashMap<Integer, WorkerSlot> taskNodeport;

    // broadcast tasks for each stream
    private DownstreamTasks downStreamTasks;

    private Lock lock = new ReentrantLock();

    public TaskTransfer(Task task, String taskName,
            KryoTupleSerializer serializer, TaskStatus taskStatus,
            WorkerData workerData, int thisTaskId) {
        this.task = task;
        this.taskName = taskName;
        this.serializer = serializer;
        this.taskStatus = taskStatus;
        this.storm_conf = workerData.getStormConf();
        this.transferQueue = workerData.getTransferQueue();
        this.innerTaskTransfer = workerData.getInnerTaskTransfer();
        this.taskId = thisTaskId;
        
        this.nodeportSocket = workerData.getNodeportSocket();
        this.taskNodeport = workerData.getTaskNodeport();

        int queue_size =
                Utils.getInt(storm_conf
                        .get(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE));
        WaitStrategy waitStrategy =
                (WaitStrategy) JStormUtils.createDisruptorWaitStrategy(storm_conf);
        this.serializeQueue =
                DisruptorQueue.mkInstance(taskName, ProducerType.MULTI,
                        queue_size, waitStrategy);
        this.serializeQueue.consumerStarted();

        String taskId = taskName.substring(taskName.indexOf(":") + 1);
        String metricName =
                MetricRegistry.name(MetricDef.SERIALIZE_QUEUE, taskName);
        QueueGauge serializeQueueGauge =
                new QueueGauge(metricName, serializeQueue);
        JStormMetrics.registerTaskGauge(serializeQueueGauge,
                Integer.valueOf(taskId), MetricDef.SERIALIZE_QUEUE);
        JStormHealthCheck.registerTaskHealthCheck(Integer.valueOf(taskId),
                MetricDef.SERIALIZE_QUEUE, serializeQueueGauge);
        timer =
                JStormMetrics.registerTaskTimer(Integer.valueOf(taskId),
                        MetricDef.SERIALIZE_TIME);

        serializeThread = setupSerializeThread();
        LOG.info("Successfully start TaskTransfer thread");

    }

    public void setDownStreamTasks(DownstreamTasks downStreamTasks) {
        this.downStreamTasks = downStreamTasks;
    }

    public void transfer(TupleExt tuple) {
        int targetTaskId = tuple.getTargetTaskId();
        // this is equal to taskId
        int sourceTaskId = tuple.getSourceTask();
        GlobalTaskId globalStreamId = new GlobalTaskId(sourceTaskId, tuple.getSourceStreamId());

        // first check weather we need to skip
        if (downStreamTasks.isSkip(globalStreamId, sourceTaskId, targetTaskId)) {
//            LOG.info("Skipping transfer of tuple {} --> {}", sourceTaskId, targetTaskId);
            return;
        }

        // we will get the target is no mapping
        int mapping = downStreamTasks.getMapping(globalStreamId, sourceTaskId, targetTaskId);
        // LOG.info("Got a mapping of task transfer {} --> {}", targetTaskId, mapping);
        StringBuilder innerTaskTextMsg = new StringBuilder();
        StringBuilder outerTaskTextMsg = new StringBuilder();
        DisruptorQueue exeQueue = innerTaskTransfer.get(mapping);
        if (exeQueue != null) {
            // in this case we are not going to hit TaskReceiver, so we need to do what we did there
            // lets determine weather we need to send this message to other tasks as well acting as an intermediary
            Map<GlobalTaskId, Set<Integer>> downsTasks = downStreamTasks.allDownStreamTasks(mapping);
            // LOG.info("Task TRANSFER taskId: {}, down tasks: {}", mapping, DownstreamTasks.printDownTasks(downsTasks));
            if (downsTasks != null && downsTasks.containsKey(globalStreamId) && !downsTasks.get(globalStreamId).isEmpty()) {
                Set<Integer> tasks = downsTasks.get(globalStreamId);
//                LOG.info("TRANSFER tasks: {}", tasks);
                byte[] tupleMessage = null;

                for (Integer task : tasks) {
                    if (task != mapping) {
                        // these tasks can be in the same worker or in a different worker
                        DisruptorQueue exeQueueNext = innerTaskTransfer.get(task);
                        if (exeQueueNext != null) {
                            innerTaskTextMsg.append(task).append(" ");
                            exeQueueNext.publish(tuple);
                        } else {
                            outerTaskTextMsg.append(task).append(" ");
                            if (tupleMessage == null) {
                                lock.lock();
                                try {
                                    tupleMessage = serializer.serialize(tuple);
                                } finally {
                                    lock.unlock();
                                }
                            }
                            TaskMessage taskMessage = new TaskMessage(task, tupleMessage, tuple.getSourceTask(), tuple.getSourceStreamId());
                            IConnection conn = getConnection(task);
                            if (conn != null) {
                                conn.send(taskMessage);
                            }
                        }
                    } else {
                        innerTaskTextMsg.append(task).append(" ");
                        exeQueue.publish(tuple);
                    }
                }
                //LOG.info("TRANSFER: Sending downstream message from task " + mapping + " [" + "inner tasks: " + innerTaskTextMsg + " outer tasks: " + outerTaskTextMsg + "]");
            } else {
//                LOG.info("Executing task, No Downstream task for message with stream ID: " + globalStreamId);
                exeQueue.publish(tuple);
            }
        } else {
//            LOG.info("Transferring tuple via network {} --> {}", sourceTaskId, targetTaskId);
            int taskid = tuple.getTargetTaskId();
            byte[] tupleMessage;
            lock.lock();
            try {
                tupleMessage = serializer.serialize(tuple);
            } finally {
                lock.unlock();
            }
            TaskMessage taskMessage = new TaskMessage(taskid, tupleMessage,
                    tuple.getSourceTask(), tuple.getSourceStreamId());
            IConnection conn = getConnection(taskid);
            if (conn != null) {
                conn.send(taskMessage);
            }
        }
    }

    public void transfer(byte []tuple, int task, int sourceTask, String streamId) {
        TaskMessage taskMessage = new TaskMessage(task, tuple, sourceTask, streamId);
        IConnection conn = getConnection(task);
        if (conn != null) {
            conn.send(taskMessage);
        } else {
            String s = "No connection to task: " + task;
            LOG.error(s);
            throw new RuntimeException(s);
        }
    }

    public void transfer(byte []tuple, Tuple tupleExt, int task) {
        DisruptorQueue exeQueue = innerTaskTransfer.get(task);
        if (exeQueue == null) {
            TaskMessage taskMessage = new TaskMessage(task, tuple);
            IConnection conn = getConnection(task);
            if (conn != null) {
                conn.send(taskMessage);
            } else {
                String s = "No connection to task: " + task;
                LOG.error(s);
                throw new RuntimeException(s);
            }
        } else {
            exeQueue.publish(tupleExt);
        }
    }

    protected AsyncLoopThread setupSerializeThread() {
        return new AsyncLoopThread(new TransferRunnable());
    }

    public AsyncLoopThread getSerializeThread() {
        return serializeThread;
    }

    protected class TransferRunnable extends RunnableCallback implements EventHandler {

        private AtomicBoolean shutdown = AsyncLoopRunnable.getShutdown();

        @Override
        public String getThreadName() {
            return taskName + "-" + TransferRunnable.class.getSimpleName();
        }

        @Override
        public void preRun() {
            WorkerClassLoader.switchThreadContext();
        }

        @Override
        public void run() {

            while (shutdown.get() == false) {
                serializeQueue.consumeBatchWhenAvailable(this);

            }

        }

        @Override
        public void postRun() {
            WorkerClassLoader.restoreThreadContext();
        }

        @Override
        public void onEvent(Object event, long sequence, boolean endOfBatch)
                throws Exception {

            if (event == null) {
                return;
            }

            long start = System.currentTimeMillis();

            try {
                TupleExt tuple = (TupleExt) event;
                int taskid = tuple.getTargetTaskId();
                byte[] tupleMessage;
                lock.lock();
                try {
                    tupleMessage = serializer.serialize(tuple);
                } finally {
                    lock.unlock();
                }
                TaskMessage taskMessage = new TaskMessage(taskid, tupleMessage,
                        tuple.getSourceTask(), tuple.getSourceStreamId());
                IConnection conn = getConnection(taskid);
                if (conn != null) {
                    conn.send(taskMessage);
                }
            } finally {
                long end = System.currentTimeMillis();
                timer.update(end - start);
            }

        }
    }

    protected IConnection getConnection(int taskId) {
        IConnection conn = null;
        WorkerSlot nodePort = taskNodeport.get(taskId);
        if (nodePort == null) {
            String errormsg = "can`t not found IConnection to " + taskId;
            LOG.warn("Intra transfer warn", new Exception(errormsg));
        } else {
            conn = nodeportSocket.get(nodePort);
            if (conn == null) {
                String errormsg = "can`t not found nodePort " + nodePort;
                LOG.warn("Intra transfer warn", new Exception(errormsg));
            }
        }
        return conn;
    }

}
