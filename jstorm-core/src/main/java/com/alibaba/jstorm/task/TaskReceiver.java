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

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.tuple.BatchTuple;
import com.alibaba.jstorm.daemon.worker.timer.TimerTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.serialization.KryoTupleDeserializer;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.client.ConfigExtension;
import com.alibaba.jstorm.common.metric.Histogram;
import com.alibaba.jstorm.common.metric.QueueGauge;
import com.alibaba.jstorm.metric.JStormHealthCheck;
import com.alibaba.jstorm.metric.JStormMetrics;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.utils.JStormUtils;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

public class TaskReceiver {
    private static Logger LOG = LoggerFactory.getLogger(TaskReceiver.class);

    protected Task task;
    protected final int taskId;
    protected final String idStr;

    protected TopologyContext topologyContext;
    protected Map<Integer, DisruptorQueue> innerTaskTransfer;

    protected final boolean isDebugRecv;

    protected DisruptorQueue deserializeQueue;
    protected KryoTupleDeserializer deserializer;
    protected AsyncLoopThread deserializeThread;
    protected Histogram deserializeTimer;

    protected TaskStatus taskStatus;

    // if this task works as a broadcast intermediary, what are the tasks we need to relay this message
    // the tasks are grouped with stream id
    protected DownstreamTasks downStreamTasks;

    protected TaskTransfer taskTransfer;

    public TaskReceiver(Task task, int taskId, Map stormConf,
            TopologyContext topologyContext,
            Map<Integer, DisruptorQueue> innerTaskTransfer,
            TaskStatus taskStatus, String taskName,
            DownstreamTasks downStreamTasks, TaskTransfer taskTransfer) {
        this.task = task;
        this.taskId = taskId;
        this.idStr = taskName;

        this.topologyContext = topologyContext;
        this.innerTaskTransfer = innerTaskTransfer;

        this.taskStatus = taskStatus;
        this.taskTransfer = taskTransfer;
        this.downStreamTasks = downStreamTasks;

        this.isDebugRecv = ConfigExtension.isTopologyDebugRecvTuple(stormConf);

        int queueSize =
                JStormUtils
                        .parseInt(
                                stormConf
                                        .get(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE),
                                256);

        WaitStrategy waitStrategy =
                (WaitStrategy) JStormUtils
                        .createDisruptorWaitStrategy(stormConf);
        this.deserializeQueue =
                DisruptorQueue.mkInstance("TaskDeserialize",
                        ProducerType.MULTI, queueSize, waitStrategy);
        setDeserializeThread();
        this.deserializer =
                new KryoTupleDeserializer(stormConf, topologyContext);

        deserializeTimer =
                JStormMetrics.registerTaskHistogram(taskId,
                        MetricDef.DESERIALIZE_TIME);

        QueueGauge deserializeQueueGauge =
                new QueueGauge(idStr + MetricDef.DESERIALIZE_QUEUE,
                        deserializeQueue);
        JStormMetrics.registerTaskGauge(deserializeQueueGauge, taskId,
                MetricDef.DESERIALIZE_QUEUE);
        JStormHealthCheck.registerTaskHealthCheck(taskId,
                MetricDef.DESERIALIZE_QUEUE, deserializeQueueGauge);
    }

    public AsyncLoopThread getDeserializeThread() {
        return deserializeThread;
    }

    protected void setDeserializeThread() {
        this.deserializeThread =
                new AsyncLoopThread(new DeserializeRunnable(deserializeQueue,
                        innerTaskTransfer.get(taskId)));
    }

    public DisruptorQueue getDeserializeQueue() {
        return deserializeQueue;
    }

    class DeserializeRunnable extends RunnableCallback implements EventHandler {

        DisruptorQueue deserializeQueue;
        DisruptorQueue exeQueue;

        DeserializeRunnable(DisruptorQueue deserializeQueue,
                DisruptorQueue exeQueue) {
            this.deserializeQueue = deserializeQueue;
            this.exeQueue = exeQueue;
        }

        @Override
        public String getThreadName() {
            return idStr + "-deserializer";
        }

        protected Object deserialize(byte[] ser_msg) {
            long start = System.nanoTime();
            try {
                if (ser_msg == null) {
                    return null;
                }

                if (ser_msg.length == 0) {
                    return null;
                } else if (ser_msg.length == 1) {
                    byte newStatus = ser_msg[0];
                    LOG.info("Change task status as " + newStatus);
                    taskStatus.setStatus(newStatus);

                    return null;
                }

                // ser_msg.length > 1
                Tuple tuple = deserializer.deserialize(ser_msg);

                if (isDebugRecv) {
                    LOG.info(idStr + " receive " + tuple.toString());
                }

                return tuple;
            } catch (Throwable e) {
                if (taskStatus.isShutdown() == false) {
                    LOG.error(
                            idStr + " recv thread error "
                                    + JStormUtils.toPrintableString(ser_msg)
                                    + "\n", e);
                }
            } finally {
                long end = System.nanoTime();
                deserializeTimer.update((end - start)/1000000.0d);
            }

            return null;
        }

        @Override
        public void onEvent(Object event, long sequence, boolean endOfBatch)
                throws Exception {
            Object tuple = deserialize((byte[]) event);
            if (tuple instanceof Tuple) {
                processTupleEvent((Tuple) tuple, event);
            } else if (tuple instanceof BatchTuple) {
                for (Tuple t : ((BatchTuple) tuple).getTuples()) {
                    processTupleEvent(t, event);
                }
            } else if (tuple instanceof TimerTrigger.TimerEvent) {
                exeQueue.publish(event);
            } else {
                LOG.warn("Received unknown message");
            }

        }

        private void processTupleEvent(Tuple tuple, Object event) {
            if (tuple != null) {
                String streamId = tuple.getSourceStreamId();
                String sourceCompoent = tuple.getSourceComponent();
                int sourceTask = tuple.getSourceTask();
                GlobalStreamId globalStreamId = new GlobalStreamId(sourceCompoent, streamId);
                LOG.info("Received message with stream ID: {} sourceTask {}", globalStreamId, sourceTask);
                // lets determine weather we need to send this message to other tasks as well acting as an intermediary
                Map<GlobalStreamId, Set<Integer>> downsTasks = downStreamTasks.allDownStreamTasks(taskId);
                if (downsTasks != null && downsTasks.containsKey(globalStreamId) && !downsTasks.get(globalStreamId).isEmpty()) {
                    // for now lets use the deserialized task and send it back... ideally we should send the byte message
                    Set<Integer> tasks = downsTasks.get(globalStreamId) ;
                    StringBuilder innerTaskTextMsg = new StringBuilder();
                    StringBuilder outerTaskTextMsg = new StringBuilder();
                    for (Integer task : tasks) {
                        if (task != taskId) {
                            // these tasks can be in the same worker or in a different worker
                            DisruptorQueue exeQueueNext = innerTaskTransfer.get(task);
                            if (exeQueueNext != null) {
                                innerTaskTextMsg.append(task).append(" ");
                                exeQueueNext.publish(tuple);
                            } else {
                                outerTaskTextMsg.append(task).append(" ");
                                taskTransfer.transfer((byte[]) event, tuple, task);
                            }
                        } else {
                            innerTaskTextMsg.append(task).append(" ");
                            exeQueue.publish(tuple);
                        }
                    }

                    if (LOG.isInfoEnabled()) {
                        StringBuilder sb = new StringBuilder("RECEIVE: Sending downstream message from task ").append(topologyContext.getThisTaskId()).append(" [");
                        sb.append("inner tasks: ").append(innerTaskTextMsg).append(" outer tasks: ").append(outerTaskTextMsg);
                        sb.append("]");
                        LOG.info(sb.toString());
                    }
                } else {
                    LOG.info("No Downstream task for message with stream ID: " + globalStreamId);
                    exeQueue.publish(tuple);
                }
            }
        }

        @Override
        public void preRun() {
            WorkerClassLoader.switchThreadContext();  
        }

        @Override
        public void postRun() {
            WorkerClassLoader.restoreThreadContext();
        }

        @Override
        public void run() {
            deserializeQueue.consumerStarted();
            LOG.info("Successfully start recvThread of " + idStr);

            while (taskStatus.isShutdown() == false) {
                try {

                    deserializeQueue.consumeBatchWhenAvailable(this);
                } catch (Throwable e) {
                    if (taskStatus.isShutdown() == false) {
                        LOG.error("Unknow exception ", e);
                    }
                }

            }

            task.unregisterDeserializeQueue();

            LOG.info("Successfully shutdown recvThread of " + idStr);
        }

        public Object getResult() {
            LOG.info("Begin to shutdown recvThread of " + idStr);
            return -1;
        }

    }
}