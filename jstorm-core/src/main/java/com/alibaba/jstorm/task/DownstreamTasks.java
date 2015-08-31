package com.alibaba.jstorm.task;

import backtype.storm.generated.GlobalStreamId;

import java.util.*;

public class DownstreamTasks {
    private Map<GlobalStreamId, List<CommunicationTree>> expandingTrees = new HashMap<GlobalStreamId, List<CommunicationTree>>();

    private Map<GlobalStreamId, List<CommunicationTree>> nonExpandingTrees = new HashMap<GlobalStreamId, List<CommunicationTree>>();

    /**
     * Given this task ID, target task and stream id, get the tasks we need to send in order to reach the target eventually
     * @param targetTask target task
     * @param thisId this task
     * @param streamId stream id
     * @return the tasks
     */
    public Set<Integer> getMappingTasks(int targetTask, int thisId, GlobalStreamId streamId) {
        TreeSet<Integer> returnTasks = new TreeSet<Integer>();

        List<CommunicationTree> expandingTreeList = expandingTrees.get(streamId);
        // we found this in an expanding list
        if (expandingTreeList != null) {
            for (CommunicationTree tree : expandingTreeList) {
                Set<Integer> downsTreamTasks = tree.getChildTasks(thisId);
                if (downsTreamTasks.contains(targetTask)) {
                    returnTasks.add(targetTask);
                }
            }
        }

        // now go through the non expanding list
        List<CommunicationTree> nonExpandingTreeList = nonExpandingTrees.get(streamId);
        if (nonExpandingTreeList != null) {
            for (CommunicationTree tree : nonExpandingTreeList) {
                Set<Integer> rootTasks = tree.rootTasks();
                if (rootTasks.contains(targetTask)) {
                    Set<Integer> downStreamTasks = tree.getChildTasks(thisId);
                    returnTasks.addAll(downStreamTasks);
                }
            }
        }

        return returnTasks;
    }

    /**
     * Return true if this task only relays the tuple without acting on it
     * @param taskId task id
     * @param streamId stream id
     * @return true if task only relays the tuple
     */
    public boolean isRelayingTuple(int taskId, GlobalStreamId streamId) {
        if (expandingTrees.containsKey(streamId)) {
            List<CommunicationTree> trees = expandingTrees.get(streamId);
            for (CommunicationTree tree : trees) {
                if (!tree.rootTasks().contains(taskId)) {
                    return false;
                }
            }
            return true;
        }

        if (nonExpandingTrees.containsKey(streamId)) {
            List<CommunicationTree> trees = nonExpandingTrees.get(streamId);
            for (CommunicationTree tree : trees) {
                if (tree.rootTasks().contains(taskId)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public Set<Integer> getDownstreamTask(int taskId, GlobalStreamId streamId) {
        // first check the expanding trees
        TreeSet<Integer> returntasks = new TreeSet<Integer>();
        List<CommunicationTree> expandingTreeList = expandingTrees.get(streamId);
        if (expandingTreeList != null) {
            for (CommunicationTree tree : expandingTreeList) {
                Set<Integer> downsTreamTasks = tree.getChildTasks(taskId);
                returntasks.addAll(downsTreamTasks);
            }
        }

        // now go through the non expanding list
        List<CommunicationTree> nonExpandingTreeList = nonExpandingTrees.get(streamId);
        if (nonExpandingTreeList != null) {
            for (CommunicationTree tree : nonExpandingTreeList) {
                Set<Integer> downsTreamTasks = tree.getChildTasks(taskId);
                returntasks.addAll(downsTreamTasks);
            }
        }

        return returntasks;
    }

    /**
     * Add a collective tree
     * @param id stream id
     * @param tree tree
     */
    public void addCollectiveTree(GlobalStreamId id, CommunicationTree tree) {
        // we keep the expanding trees and non expanding trees in two separate maps
        if (tree.isExpandingTree()) {
            List<CommunicationTree> trees = expandingTrees.get(id);
            if (trees == null) {
                trees = new ArrayList<CommunicationTree>();
                expandingTrees.put(id, trees);
            }
            trees.add(tree);
        } else {
            List<CommunicationTree> trees = nonExpandingTrees.get(id);
            if (trees == null) {
                trees = new ArrayList<CommunicationTree>();
                nonExpandingTrees.put(id, trees);
            }
            trees.add(tree);
        }
    }

    /**
     * Get all the downstream tasks of a given task
     * @param taskId task id
     * @return a map of all the downstream tasks, keyed by the stream and component id
     */
    public Map<GlobalStreamId, Set<Integer>> allDownStreamTasks(int taskId) {
        Map<GlobalStreamId, Set<Integer>> allTasks = new HashMap<GlobalStreamId, Set<Integer>>();
        for (Map.Entry<GlobalStreamId, List<CommunicationTree>> e : expandingTrees.entrySet()) {
            GlobalStreamId id = e.getKey();
            List<CommunicationTree> treeList = e.getValue();
            TreeSet<Integer> treeSet = new TreeSet<Integer>();
            for (CommunicationTree t : treeList) {
                treeSet.addAll(t.getChildTasks(taskId));
            }
            allTasks.put(id, treeSet);
        }
        for (Map.Entry<GlobalStreamId, List<CommunicationTree>> e : nonExpandingTrees.entrySet()) {
            GlobalStreamId id = e.getKey();
            List<CommunicationTree> treeList = e.getValue();
            Set<Integer> treeSet = allTasks.get(id);
            if (treeSet == null) {
                treeSet = new TreeSet<Integer>();
                allTasks.put(id, treeSet);
            }
            for (CommunicationTree t : treeList) {
                treeSet.addAll(t.getChildTasks(taskId));
            }
        }
        return allTasks;
    }
}
