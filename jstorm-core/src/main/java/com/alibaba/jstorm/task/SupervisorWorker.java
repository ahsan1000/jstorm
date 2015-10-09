package com.alibaba.jstorm.task;

import java.util.ArrayList;
import java.util.List;

public class SupervisorWorker {
    private String supervisorId;

    private List<WorkerTask> workerTasksList = new ArrayList<WorkerTask>();

    public SupervisorWorker(String supervisorId, List<WorkerTask> workerTasksList) {
        this.supervisorId = supervisorId;
        this.workerTasksList = workerTasksList;
    }

    public String getSupervisorId() {
        return supervisorId;
    }

    public List<WorkerTask> getWorkerTasksList() {
        return workerTasksList;
    }
}


