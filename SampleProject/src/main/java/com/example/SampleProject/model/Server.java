package com.example.SampleProject.model;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * this is model class to represent the server.
 */

public class Server implements Serializable,Comparable<Server> {
    private int serverId;
    private int freeSize;
    private int ram;
    private String state;

    public Server(int freeSize, String state, int serverId) {
        this.ram=100;
        this.freeSize = freeSize;
        this.state = state;
        this.serverId = serverId;
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public int getFreeSize() {
        return freeSize;
    }

    public void setFreeSize(int freeSize) {
        this.freeSize = freeSize;
    }

    public int getRam() {
        return ram;
    }

    public void setRam(int ram) {
        this.ram = ram;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }


    @Override
    public int compareTo(Server o) {
        if (this.getServerId() > o.getServerId()) {
            return 1;
        } else if (this.getServerId() < o.getServerId()) {
            return -1;
        }
        return 0;
    }

}
