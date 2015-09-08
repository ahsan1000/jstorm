package com.alibaba.jstorm.task;

import backtype.storm.Config;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a collective communications tree. The parameters of the tree defines how the collective communications happen.
 */
public class CommunicationTree {
    private static Logger LOG = LoggerFactory.getLogger(CommunicationTree.class);
    /**
     * Weather we have a expanding tree or a gathering tree. These operations are from n parallel tasks to m parallel tasks.
     * Expanding trees are when n < m and gathering trees are when n > m
     */
    private boolean expandingTree;

    /**
     * How many branches we permit at the worker level
     */
    private int workerLevelBranchingFactor = 2;

    /**
     * How many branches we permit at the node level
     */
    private int nodeLevelBranchingFactor = 2;

    /**
     * A node in the collective tree
     */
    private class TreeNode {
        List<TreeNode> children = new ArrayList<TreeNode>();
        TreeNode parent;
        TreeSet<Integer> taskIds = new TreeSet<Integer>();
        TreeNode left() {
            if (children.size() > 0) {
                return children.get(0);
            }
            return null;
        }
        TreeNode right() {
            if (children.size() > 1) {
                return children.get(1);
            }
            return null;
        }
    }

    /** Root of this tree */
    private TreeNode root;

    public CommunicationTree(Map conf, TreeMap<String, TreeMap<Integer, TreeSet<Integer>>> mappings, boolean expandingTree) {
        this(conf, null, mappings, expandingTree);
    }

    public boolean isExpandingTree() {
        return expandingTree;
    }

    public TreeSet<Integer> rootTasks() {
        return root.taskIds;
    }

    /**
     * Create a collective tree
     * @param conf topology configuration
     */
    public CommunicationTree(Map conf, Set<Integer> rootTaskId, TreeMap<String, TreeMap<Integer, TreeSet<Integer>>> mappings, boolean expandingTree) {
        StringBuilder sb = new StringBuilder("Root Tasks: ");
        for (Integer t : rootTaskId) {
            sb.append(t).append(" ");
        }
        sb.append("\n");
        String s = CommunicationPlanner.printMappings(mappings);
        sb.append("Mappings: ").append(s);


        LOG.info("Collective tree: " + sb.toString());
        Integer workerLevelBranching = (Integer) conf.get(Config.COLLECTIVE_WORKER_BRANCHING_FACTOR);
        if (workerLevelBranching != null) {
            this.workerLevelBranchingFactor = workerLevelBranching;
        }

        Integer nodeLevelBranching = (Integer) conf.get(Config.COLLECTIVE_NODE_BRANCHING_FACTOR);
        if (nodeLevelBranching != null) {
            this.nodeLevelBranchingFactor = nodeLevelBranching;
        }
        LOG.info("NodeLevel branching: {}, WorkerLevel Branching: {}", this.nodeLevelBranchingFactor, this.workerLevelBranchingFactor);
        this.expandingTree = expandingTree;

        root = new TreeNode();
        root.taskIds.addAll(rootTaskId);
        buildTree(root, mappings);
        BTreePrinter.printNode(root);
    }

    public TreeSet<Integer> taskIdsOfNode(int taskId) {
        TreeNode treeNode = searchTree(root, taskId);
        if (treeNode != null) {
            return treeNode.taskIds;
        } else {
            return null;
        }
    }

    /**
     * Search for tree
     * @param node node of the tree
     * @param item task id
     * @return the node, null if not found
     */
    private TreeNode searchTree(TreeNode node, int item) {
        Queue<TreeNode> treeNodes = new LinkedList<TreeNode>();
        treeNodes.offer(node);
        while (!treeNodes.isEmpty()) {
            TreeNode n = treeNodes.poll();

            // if this is the node return it
            if (n.taskIds.contains(item)) {
                return n;
            }

            // expand the children
            for (TreeNode tn : n.children) {
                treeNodes.offer(tn);
            }
        }
        return null;
    }

    /**
     * Build the whole collective tree
     * @param parent the starting node
     * @param tasks worker to task mapping in nodes, all these tasks are connected using collective operations
     */
    private void buildTree(TreeNode parent, TreeMap<String, TreeMap<Integer, TreeSet<Integer>>> tasks) {
        NavigableSet<String> treeMap = tasks.navigableKeySet();
        Iterator<String> itr = treeMap.iterator();

        Queue<TreeNode> treeNodes = new LinkedList<TreeNode>();
        treeNodes.offer(parent);

        TreeNode p = treeNodes.poll();
        int nodeCount = 0;

        while (itr.hasNext()) {
            String nodes = itr.next();
            TreeMap<Integer, TreeSet<Integer>> workers = tasks.get(nodes);
            TreeNode n = buildTreeOfNode(p, workers);
            p.children.add(n);
            treeNodes.offer(n);
            nodeCount++;

            if (nodeCount == nodeLevelBranchingFactor) {
                nodeCount = 0;
                p = treeNodes.poll();
            }
        }
    }

    /**
     * Build the part of the tree specific to the node
     * @param parent parent node
     * @param tasks worker to task mapping in this node, all these tasks are connected using collective operations
     * @return the first node of this sub tree
     */
    private TreeNode buildTreeOfNode(TreeNode parent, TreeMap<Integer, TreeSet<Integer>> tasks) {
        NavigableSet<Integer> treeMap = tasks.navigableKeySet();
        Iterator<Integer> itr = treeMap.iterator();

        TreeNode returnParent = null;

        int count = 0;
        int level = 0;
        Queue<TreeNode> treeNodes = new LinkedList<TreeNode>();
        TreeNode p = parent;
        while (itr.hasNext()) {
            Integer w = itr.next();
            TreeSet<Integer> t = tasks.get(w);

            TreeNode treeNode = new TreeNode();
            treeNode.parent = p;

            p.children.add(treeNode);
            treeNode.taskIds.addAll(t);
            count++;
            treeNodes.offer(treeNode);

            if (level == 0) {
                returnParent = treeNode;
                level++;
                count = 0;
                p = treeNodes.poll();
                continue;
            }

            // we are done with this node
            if (count == workerLevelBranchingFactor) {
                count = 0;
                p = treeNodes.poll();
            }
        }

        return returnParent;
    }

    /**
     * After building the tree, we can use this method to query the tasks to which we need to communicate.
     * 1. We will send the data to 1 worker in each node. The task in that worker will
     * broadcast the message to other workers in the same machine
     *
     * @return task ids,
     */
    public TreeSet<Integer> getChildTasks(int taskId) {
        TreeNode t = searchTree(root, taskId);
        if (t != null) {
            if (expandingTree) {
                TreeSet<Integer> treeSet = new TreeSet<Integer>();
                // check weather taskId is the first of the node, if not first we don't have downstream tasks
                int first = t.taskIds.first();
                // we return empty set
                if (taskId != first) {
                    return treeSet;
                } else {
                    for (Integer task : t.taskIds) {
                        treeSet.add(task);
                    }
                }

                // if we have child nodes, add the first of each child node as well
                for (TreeNode tn : t.children) {
                    // add the first of the childrens task ids
                    treeSet.add(tn.taskIds.first());
                }

                return treeSet;
            } else {
                // in case of non-expanding tree we only have one child
                TreeSet<Integer> treeSet = new TreeSet<Integer>();
                int first = t.taskIds.first();
                // if we are the first task, then we only have child nodes from the parent
                if (taskId == first) {
                    TreeNode parent = t.parent;
                    if (parent != null) {
                        treeSet.add(parent.taskIds.first());
                        return treeSet;
                    }
                    return treeSet;
                } else {
                    for (Integer task : t.taskIds) {
                        // add the first tasks as children
                        if (task == taskId) {
                            treeSet.add(first);
                        }
                    }
                }
                return treeSet;
            }
        }
        return new TreeSet<Integer>();
    }

    static class BTreePrinter {
        public static void printNode(TreeNode root) {
            int maxLevel = BTreePrinter.maxLevel(root);
            LOG.info("Max level: {}", 3);
            String out = printNodeInternal(Collections.singletonList(root), 1, maxLevel);
            LOG.info(out);
        }

        private static String printNodeInternal(List<TreeNode> nodes, int level, int maxLevel) {
            if (nodes.isEmpty() || BTreePrinter.isAllElementsNull(nodes))
                return "";

            int floor = maxLevel - level;
            int endgeLines = (int) Math.pow(2, (Math.max(floor - 1, 0)));
            int firstSpaces = (int) Math.pow(2, (floor)) - 1;
            int betweenSpaces = (int) Math.pow(2, (floor + 1)) - 1;
            String out = "";
            out += BTreePrinter.printWhitespaces(firstSpaces);
            List<TreeNode> newNodes = new ArrayList<TreeNode>();
            for (TreeNode node : nodes) {
                if (node != null) {
                    out += node.taskIds;
                    newNodes.add(node.left());
                    newNodes.add(node.right());
                } else {
                    newNodes.add(null);
                    newNodes.add(null);
                    out += " ";
                }

                out += BTreePrinter.printWhitespaces(betweenSpaces);
            }
            out += "\n";

            for (int i = 1; i <= endgeLines; i++) {
                for (int j = 0; j < nodes.size(); j++) {
                    BTreePrinter.printWhitespaces(firstSpaces - i);
                    if (nodes.get(j) == null) {
                        out += BTreePrinter.printWhitespaces(endgeLines + endgeLines + i + 1);
                        continue;
                    }

                    if (nodes.get(j).left() != null)
                        out += "/";
                    else
                        out += BTreePrinter.printWhitespaces(1);

                    out += BTreePrinter.printWhitespaces(i + i - 1);

                    if (nodes.get(j).right() != null)
                        out+="\\";
                    else
                        out += BTreePrinter.printWhitespaces(1);

                    out += BTreePrinter.printWhitespaces(endgeLines + endgeLines - i);
                }

                out += "\n";
            }

            out += printNodeInternal(newNodes, level + 1, maxLevel);
            return out;
        }

        private static String printWhitespaces(int count) {
            String s = "";
            for (int i = 0; i < count; i++)
                s += (" ");

            return s;
        }

        private static <T extends Comparable<?>> int maxLevel(TreeNode node) {
            if (node == null)
                return 0;

            return Math.max(BTreePrinter.maxLevel(node.left()), BTreePrinter.maxLevel(node.right())) + 1;
        }

        private static <T> boolean isAllElementsNull(List<T> list) {
            for (Object object : list) {
                if (object != null)
                    return false;
            }

            return true;
        }

    }
}
