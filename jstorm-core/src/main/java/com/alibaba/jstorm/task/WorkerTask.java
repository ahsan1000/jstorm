package com.alibaba.jstorm.task;

import java.util.ArrayList;
import java.util.List;

public class WorkerTask implements Comparable {
    private int port;
    private List<Integer> tasks = new ArrayList<Integer>();

    public WorkerTask(int port, List<Integer> tasks) {
        this.port = port;
        this.tasks = tasks;
    }

    public int getPort() {
        return port;
    }

    public List<Integer> getTasks() {
        return tasks;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }
}
