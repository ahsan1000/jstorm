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
    private static class TreeNode {
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

        Boolean useFlatTree = (Boolean) conf.get(Config.COLLECTIVE_USE_FLAT_TREE);

        LOG.info("Collective tree: " + sb.toString());
        Object workerLevelBranching = conf.get(Config.COLLECTIVE_WORKER_BRANCHING_FACTOR);
        if (workerLevelBranching != null) {
            if (workerLevelBranching instanceof Integer) {
                this.workerLevelBranchingFactor = (int) workerLevelBranching;
            }
            if (workerLevelBranching instanceof Long) {
                long w = (long) workerLevelBranching;
                this.workerLevelBranchingFactor = (int) w;
            }
        }

        Integer nodeLevelBranching = (Integer) conf.get(Config.COLLECTIVE_NODE_BRANCHING_FACTOR);
        // if we are going to use a flat tree no level branching factor should be the size of supervisors
        if (useFlatTree != null && useFlatTree) {
            this.nodeLevelBranchingFactor = mappings.size();
        } else {
            if (nodeLevelBranching != null) {
                this.nodeLevelBranchingFactor = nodeLevelBranching;
            }
        }

        LOG.info("NodeLevel branching: {}, WorkerLevel Branching: {}", this.nodeLevelBranchingFactor, this.workerLevelBranchingFactor);
        this.expandingTree = expandingTree;

        root = new TreeNode();
        root.taskIds.addAll(rootTaskId);
        buildTree(root, mappings);
        LOG.info("Tree: {}", BTreePrinter.print(root));
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
            TreeNode n = buildTreeOfNode(workers);
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
     * @param tasks worker to task mapping in this node, all these tasks are connected using collective operations
     * @return the first node of this sub tree
     */
    private TreeNode buildTreeOfNode(TreeMap<Integer, TreeSet<Integer>> tasks) {
        NavigableSet<Integer> treeMap = tasks.navigableKeySet();
        Iterator<Integer> itr = treeMap.iterator();

        TreeNode nodeRoot = new TreeNode();

        int count = workerLevelBranchingFactor;
        boolean root = true;
        Queue<TreeNode> treeNodes = new LinkedList<TreeNode>();
        treeNodes.add(nodeRoot);
        TreeNode parent = null;

        while (itr.hasNext()) {
            Integer w = itr.next();
            TreeSet<Integer> t = tasks.get(w);

            // if worker level branching factor is < 0, then we will create a flat tree
            if (count == workerLevelBranchingFactor) {
                parent = treeNodes.poll();
                count = 0;
            }

            // we need to do some special processing for root
            if (root) {
                parent.taskIds.addAll(t);
                root = false;
            } else {
                TreeNode node = new TreeNode();
                node.taskIds.addAll(t);
                // add the node to parent
                node.parent = parent;
                parent.children.add(node);
                treeNodes.offer(node);
                count++;
            }
        }
        return nodeRoot;
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
        public static String print(TreeNode root) {
            StringBuilder sb = new StringBuilder("(");
            int state = 0; // 0 = saw node, 1 = saw 1, 2 = saw 2
            Queue<Object> queue = new LinkedList<Object>();
            queue.offer(root);
            queue.offer(1);
            while(!queue.isEmpty()) {
                Object t = queue.poll();
                if (t instanceof Integer) {
                    sb.append("\n");
                    if (!queue.isEmpty()) {
                        queue.add(1);
                    }
                }

                if (t instanceof TreeNode) {
                    printNodeInternal((TreeNode) t, sb);
                    queue.addAll(((TreeNode) t).children);
                }
            }
            return sb.toString();
        }

        private static void printNodeInternal(TreeNode node, StringBuilder sb) {
            sb.append("[");
            for (Integer t : node.taskIds) {
                sb.append(t).append(" ");
            }
            sb.append("]");
        }
    }


    public static void main(String[] args) {
        TreeNode root = new TreeNode();
        root.taskIds.add(1);

        TreeNode ch1 = new TreeNode();
        ch1.taskIds.add(2);

        TreeNode ch2 = new TreeNode();
        ch2.taskIds.add(3);

        TreeNode ch3 = new TreeNode();
        ch3.taskIds.add(4);

        TreeNode ch4 = new TreeNode();
        ch4.taskIds.add(5);

        TreeNode ch5 = new TreeNode();
        ch5.taskIds.add(6);

        TreeNode ch6 = new TreeNode();
        ch6.taskIds.add(7);

        TreeNode ch7 = new TreeNode();
        ch7.taskIds.add(8);

        TreeNode ch8 = new TreeNode();
        ch8.taskIds.add(9);

        root.children.add(ch1);
        root.children.add(ch2);

        ch1.children.add(ch3);
        ch1.children.add(ch4);

        ch2.children.add(ch5);
        ch2.children.add(ch6);

        ch3.children.add(ch7);
        ch4.children.add(ch8);

        String s = BTreePrinter.print(root);
        System.out.println(s);
    }
}
