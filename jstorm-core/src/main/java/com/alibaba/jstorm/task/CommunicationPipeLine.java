package com.alibaba.jstorm.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CommunicationPipeLine {
    private Logger LOG = LoggerFactory.getLogger(CommunicationPipeLine.class);

    private int rootTask;

    private boolean nodePipe = true;

    private PipeLineNode rootNode;

    private String rootSupervisorId;

    private int rootWorkerPort;

    private class PipeLineNode {
        int previousTask; // the previous task
        int sourceTask;   // which node of this set of nodes that act as the gateway
        int targetTask;   // the target task
        Set<Integer> inMemoryTasks = new HashSet<Integer>(); // the tasks other than the source, which are in the same worker

        public PipeLineNode(int previousTask) {
            this.previousTask = previousTask;
        }

        public PipeLineNode(int previousTask, int sourceTask) {
            this.previousTask = previousTask;
            this.sourceTask = sourceTask;
        }

        public PipeLineNode(int previousTask, int sourceTask, int targetTask) {
            this.sourceTask = sourceTask;
            this.targetTask = targetTask;
            this.previousTask = previousTask;
        }

        public String serialize() {
            StringBuilder sb = new StringBuilder("p: ").append(previousTask).append(", ");
            sb.append("s: ").append(sourceTask).append(", ");
            sb.append("m: ").append(inMemoryTasks).append(", ");
            sb.append("t: ").append(targetTask);
            return sb.toString();
        }
    }

    private List<PipeLineNode> nodes = new ArrayList<PipeLineNode>();

    public CommunicationPipeLine(Map conf, int rootTask, String rootSupervisorId, int rootWorkerPort, TreeMap<String, TreeMap<Integer, TreeSet<Integer>>> mappings) {
        this.rootTask = rootTask;
        this.rootSupervisorId = rootSupervisorId;
        this.rootWorkerPort = rootWorkerPort;

        List<SupervisorWorker> supervisorWorkers = buildList(mappings);
        buildPipeLine(supervisorWorkers);

        StringBuilder sb = new StringBuilder();
        for (PipeLineNode n : nodes) {
            sb.append("[").append(n.serialize()).append("]");
        }
        LOG.info(sb.toString());
    }

    private List<SupervisorWorker> buildList(TreeMap<String, TreeMap<Integer, TreeSet<Integer>>> mappings) {

        return null;
    }

    public void buildPipeLine(List<SupervisorWorker> supervisorWorkers) {
        rootNode = new PipeLineNode(-1);
        rootNode.sourceTask = rootTask;
        nodes.add(rootNode);

        PipeLineNode currentNode = rootNode;
        for (SupervisorWorker e : supervisorWorkers) {
            List<PipeLineNode> list = buildNodePipeLine(currentNode, e.getWorkerTasksList());
            nodes.addAll(list);
            if (list.size() > 0) {
                currentNode = list.get(list.size() - 1);
            }
        }
    }

    public List<PipeLineNode> buildNodePipeLine(PipeLineNode previousTask, List<WorkerTask> nodeMapping) {
        List<PipeLineNode> pipeLineNodes = new ArrayList<PipeLineNode>();
        PipeLineNode currentPreviousTask = previousTask;

        for (WorkerTask e : nodeMapping) {
            PipeLineNode pipeLineNode = new PipeLineNode(currentPreviousTask.sourceTask);
            pipeLineNodes.add(pipeLineNode);
            List<Integer> tasks = e.getTasks();
            pipeLineNode.sourceTask = tasks.get(0);
            for (Integer t : tasks) {
                if (t != pipeLineNode.sourceTask) {
                    pipeLineNode.inMemoryTasks.add(t);
                }
            }
            // make sure last node in pipeline gets target as -1
            pipeLineNode.targetTask = -1;
            currentPreviousTask.targetTask = pipeLineNode.sourceTask;
            currentPreviousTask = pipeLineNode;
        }
        return pipeLineNodes;
    }

    public int rootTasks() {
        return rootTask;
    }

    public TreeSet<Integer> getAllTasks(int taskId) {
        TreeSet<Integer> returnTasks = new TreeSet<Integer>();
        PipeLineNode node = search(taskId);
        if (node != null) {
            LOG.info("Searched node with {}: " + node.serialize(), taskId);
            for (int t : node.inMemoryTasks) {
                if (t != taskId) {
                    returnTasks.add(t);
                }
            }
            // add the source as well
            returnTasks.add(node.sourceTask);
            // only valid targets are added, last node in pipe line
            if (node.targetTask >= 0) {
                returnTasks.add(node.targetTask);
            }
        } else {
            LOG.info("Failed to get node: " + taskId);
        }
        LOG.info("return tasks: {}", returnTasks);
        return returnTasks;
    }

    public TreeSet<Integer> getChildTasks(int taskId) {
        TreeSet<Integer> returnTasks = new TreeSet<Integer>();
        PipeLineNode node = search(taskId);
        if (node != null) {
            LOG.info("Searched node with {}: " + node.serialize(), taskId);
            if (taskId == node.sourceTask) {
                for (int t : node.inMemoryTasks) {
                    if (t != taskId) {
                        returnTasks.add(t);
                    }
                }
                // add the source as well
                returnTasks.add(node.sourceTask);
                // only valid targets are added, last node in pipe line
                if (node.targetTask >= 0) {
                    returnTasks.add(node.targetTask);
                }
            }
        } else {
            LOG.info("Failed to get node: " + taskId);
        }
        LOG.info("return tasks: {}", returnTasks);
        return returnTasks;
    }

    private PipeLineNode search(int taskId) {
        for (PipeLineNode node : nodes) {
            if (node.sourceTask == taskId || node.inMemoryTasks.contains(taskId)) {
                return node;
            }
        }
        return null;
     }
}