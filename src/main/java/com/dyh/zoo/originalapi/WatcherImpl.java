package com.dyh.zoo.originalapi;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * zookeeper watcher实现,用于监控zookeeper客户端连接
 * @author bang
 * @date 2018/5/27 17:57
 */
public class WatcherImpl implements Watcher{

    private ZooKeeper zooKeeper;
    private String connectString;

    public WatcherImpl(String connectString){
        this.connectString = connectString;
    }

    public void startZk() throws IOException {
        zooKeeper = new ZooKeeper(connectString,2000,this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws Exception {
        WatcherImpl watcher = new WatcherImpl("23.106.132.161:2181");
        watcher.startZk();
        Thread.sleep(120000);
    }

}
