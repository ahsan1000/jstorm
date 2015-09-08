package com.alibaba.jstorm.task;

import backtype.storm.generated.GlobalStreamId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DownstreamTasks {
    private Map<GlobalStreamId, List<CommunicationTree>> expandingTrees = new HashMap<GlobalStreamId, List<CommunicationTree>>();

    private Map<GlobalStreamId, List<CommunicationTree>> nonExpandingTrees = new HashMap<GlobalStreamId, List<CommunicationTree>>();

    private Map<Integer, Map<GlobalStreamId, Set<Integer>>> downstreamTaskCache = new ConcurrentHashMap<Integer, Map<GlobalStreamId, Set<Integer>>>();

    private Map<Key, Integer> mapCache = new ConcurrentHashMap<Key, Integer>();

    private Map<Key, Boolean> skipCache = new ConcurrentHashMap<Key, Boolean>();

    private class Key {
        GlobalStreamId id;
        int taskId;
        int targetId;

        private Key(GlobalStreamId id, int taskId, int targetId) {
            this.id = id;
            this.taskId = taskId;
            this.targetId = targetId;
        }

        public GlobalStreamId getId() {
            return id;
        }

        public int getTaskId() {
            return taskId;
        }

        public int getTargetId() {
            return targetId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key key = (Key) o;

            if (targetId != key.targetId) return false;
            if (taskId != key.taskId) return false;
            if (id != null ? !id.equals(key.id) : key.id != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = id != null ? id.hashCode() : 0;
            result = 31 * result + taskId;
            result = 31 * result + targetId;
            return result;
        }
    }

    public int getMapping(GlobalStreamId streamId, int taskId, int targetId) {
        Key key = new Key(streamId, taskId, targetId);
        if (mapCache.containsKey(key)) {
            return mapCache.get(key);
        } else {
            if (nonExpandingTrees.containsKey(streamId)) {
                List<CommunicationTree> trees = nonExpandingTrees.get(streamId);
                for (CommunicationTree tree : trees) {
                    if (tree.rootTasks().contains(targetId)) {
                        TreeSet<Integer> childTasks = tree.getChildTasks(taskId);
                        if (!childTasks.isEmpty()) {
                            mapCache.put(key, childTasks.first());
                            return childTasks.first();
                        }
                    }
                }
            }
            mapCache.put(key, targetId);
            return targetId;
        }
    }

    public boolean isSkip(GlobalStreamId streamId, int taskId, int targetId) {
        Key key = new Key(streamId, taskId, targetId);
        if (skipCache.containsKey(key)) {
            return  skipCache.get(key);
        } else {
            if (expandingTrees.containsKey(streamId)) {
                List<CommunicationTree> trees = expandingTrees.get(streamId);
                for (CommunicationTree tree : trees) {
                    TreeSet<Integer> childTasks = tree.getChildTasks(taskId);
                    if (!childTasks.isEmpty() && childTasks.contains(targetId)) {
                        skipCache.put(key, false);
                        return false;
                    }
                }
                skipCache.put(key, true);
                return true;
            }
            skipCache.put(key, false);
            return false;
        }
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
        if (downstreamTaskCache.containsKey(taskId)) {
            return downstreamTaskCache.get(taskId);
        } else {
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
            downstreamTaskCache.put(taskId, allTasks);
            return allTasks;
        }
    }
}
