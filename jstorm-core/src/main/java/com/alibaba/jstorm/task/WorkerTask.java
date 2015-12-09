package com.alibaba.jstorm.task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WorkerTask implements Comparable {
    int priority = 0;
    private int port;
    private List<Integer> tasks = new ArrayList<Integer>();

    public WorkerTask(int port, List<Integer> tasks) {
        this.port = port;
        this.tasks = tasks;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getPriority() {
        return priority;
    }

    public int getPort() {
        return port;
    }

    public List<Integer> getTasks() {
        return tasks;
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof WorkerTask) {
            if (this.priority == ((WorkerTask) o).priority) {
                return ((WorkerTask) o).port - this.port;
            } else {
                return ((WorkerTask) o).priority - this.priority;
            }
        }
        return 0;
    }

    public static void main(String[] args) {
        WorkerTask task1 = new WorkerTask(1, null);
        task1.setPriority(1);
        WorkerTask task2 = new WorkerTask(2, null);
        WorkerTask task3 = new WorkerTask(3, null);
        task3.setPriority(1);
        WorkerTask task4 = new WorkerTask(4, null);

        List<WorkerTask> task = new ArrayList<WorkerTask>();
        task.add(task1);
        task.add(task2);
        task.add(task3);
        task.add(task4);
        Collections.sort(task);

        for (int i = 0; i< task.size(); i++) {
            System.out.print(task.get(i).getPort());
        }
    }
}

