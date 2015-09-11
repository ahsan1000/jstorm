package com.alibaba.jstorm.task;

import java.util.Map;
import java.util.Set;

import backtype.storm.generated.GlobalStreamId;

import backtype.storm.Config;
import backtype.storm.messaging.TaskMessage;
import backtype.storm.serialization.KryoTupleSerializer;
import backtype.storm.tuple.TupleExt;
import backtype.storm.utils.DisruptorQueue;
import backtype.storm.utils.Utils;
import backtype.storm.utils.WorkerClassLoader;

import com.alibaba.jstorm.callback.AsyncLoopThread;
import com.alibaba.jstorm.callback.RunnableCallback;
import com.alibaba.jstorm.daemon.worker.WorkerData;
import com.alibaba.jstorm.metric.JStormTimer;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.metric.Metrics;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private Map storm_conf;
	private DisruptorQueue transferQueue;
	private KryoTupleSerializer serializer;
	private Map<Integer, DisruptorQueue> innerTaskTransfer;
	private DisruptorQueue serializeQueue;
	private final AsyncLoopThread serializeThread;
	private volatile TaskStatus taskStatus;
	private String taskName;
	private JStormTimer  timer;
    // broadcast tasks for each stream
    private DownstreamTasks downStreamTasks;

	public TaskTransfer(String taskName, 
			KryoTupleSerializer serializer, TaskStatus taskStatus,
			WorkerData workerData) {
		this.taskName = taskName;
		this.serializer = serializer;
		this.taskStatus = taskStatus;
		this.storm_conf = workerData.getConf();
		this.transferQueue = workerData.getTransferQueue();
		this.innerTaskTransfer = workerData.getInnerTaskTransfer();

		int queue_size = Utils.getInt(storm_conf
				.get(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE));
		WaitStrategy waitStrategy = (WaitStrategy) Utils
				.newInstance((String) storm_conf
						.get(Config.TOPOLOGY_DISRUPTOR_WAIT_STRATEGY));
		this.serializeQueue = DisruptorQueue.mkInstance(taskName, ProducerType.MULTI, 
				queue_size, waitStrategy);
		this.serializeQueue.consumerStarted();
		
		String taskId = taskName.substring(taskName.indexOf(":") + 1);
		Metrics.registerQueue(taskName, MetricDef.SERIALIZE_QUEUE, serializeQueue, taskId, Metrics.MetricType.TASK);
		timer = Metrics.registerTimer(taskName, MetricDef.SERIALIZE_TIME, taskId, Metrics.MetricType.TASK); 

		serializeThread = new AsyncLoopThread(new TransferRunnable());
		LOG.info("Successfully start TaskTransfer thread");

	}

    public void setDownStreamTasks(DownstreamTasks downStreamTasks) {
        this.downStreamTasks = downStreamTasks;
    }

    public void transfer(byte []tuple, int task) {
        TaskMessage taskMessage = new TaskMessage(task, tuple);
        transferQueue.publish(taskMessage);
    }

	public void transfer(TupleExt tuple) {
        long time = System.currentTimeMillis();
        try {
            int targetTaskId = tuple.getTargetTaskId();
            // this is equal to taskId
            int sourceTaskId = tuple.getSourceTask();
            GlobalStreamId globalStreamId = new GlobalStreamId(tuple.getSourceComponent(), tuple.getSourceStreamId());

            // first check weather we need to skip
            if (downStreamTasks.isSkip(globalStreamId, sourceTaskId, targetTaskId)) {
                // LOG.info("Skipping transfer of tuple {} --> {}", sourceTaskId, targetTaskId);
                return;
            }

            // we will get the target is no mapping
            int mapping = downStreamTasks.getMapping(globalStreamId, sourceTaskId, targetTaskId);
            // LOG.info("Got a mapping of task transfer {} --> {}", targetTaskId, mapping);
            DisruptorQueue exeQueue = innerTaskTransfer.get(mapping);
            if (exeQueue != null) {
                // LOG.info("Transferring tuple via memory {} --> {}", sourceTaskId, targetTaskId);
                // in this case we are not going to hit TaskReceiver, so we need to do what we did there
                // lets determine weather we need to send this message to other tasks as well acting as an intermediary
                Map<GlobalStreamId, Set<Integer>> downsTasks = downStreamTasks.allDownStreamTasks(mapping);
                if (downsTasks != null && downsTasks.containsKey(globalStreamId) && !downsTasks.get(globalStreamId).isEmpty()) {
                    Set<Integer> tasks = downsTasks.get(globalStreamId);
//                    StringBuilder innerTaskTextMsg = new StringBuilder();
//                    StringBuilder outerTaskTextMsg = new StringBuilder();
                    byte[] tupleMessage = null;
                    for (Integer task : tasks) {
                        if (task != mapping) {
                            // these tasks can be in the same worker or in a different worker
                            DisruptorQueue exeQueueNext = innerTaskTransfer.get(task);
                            if (exeQueueNext != null) {
//                                innerTaskTextMsg.append(task).append(" ");
                                exeQueueNext.publish(tuple);
                            } else {
//                                outerTaskTextMsg.append(task).append(" ");
                                serializeQueue.publish(tuple);
                            }
                        } else {
//                            innerTaskTextMsg.append(task).append(" ");
                            exeQueue.publish(tuple);
                        }
                    }

                    // LOG.info("TRANSFER: Sending downstream message from task " + mapping + " [" + "inner tasks: " + innerTaskTextMsg + " outer tasks: " + outerTaskTextMsg + "]");
                } else {
                    // LOG.info("No Downstream task for message with stream ID: " + globalStreamId);
                    exeQueue.publish(tuple);
                }
            } else {
                // LOG.info("Transferring tuple via network {} --> {}", sourceTaskId, targetTaskId);
                serializeQueue.publish(tuple);
            }
        } finally {
           // LOG.info("TRANSFER TIME *****: " + (System.currentTimeMillis() - time));
        }
	}

	public AsyncLoopThread getSerializeThread() {
		return serializeThread;
	}

	class TransferRunnable extends RunnableCallback implements EventHandler {
		
		@Override
		public String getThreadName() {
			return taskName + "-" +TransferRunnable.class.getSimpleName();
		}

		@Override
		public void run() {

			WorkerClassLoader.switchThreadContext();
			while (taskStatus.isShutdown() == false) {
				serializeQueue.consumeBatchWhenAvailable(this);

			}
			WorkerClassLoader.restoreThreadContext();
		}

		public Object getResult() {
			if (taskStatus.isShutdown() == false) {
				return 0;
			} else {
				return -1;
			}
		}

		@Override
		public void onEvent(Object event, long sequence, boolean endOfBatch)
				throws Exception {

			if (event == null) {
				return;
			}
			
			timer.start();

			try {
				TupleExt tuple = (TupleExt) event;
				int taskid = tuple.getTargetTaskId();
				byte[] tupleMessage = serializer.serialize(tuple);
				TaskMessage taskMessage = new TaskMessage(taskid, tupleMessage);
				transferQueue.publish(taskMessage);
			}finally {
				timer.stop();
			}
			

		}

	}

}
