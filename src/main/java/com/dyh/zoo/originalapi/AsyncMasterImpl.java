package com.dyh.zoo.originalapi;

import com.google.common.collect.Lists;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * @author bang
 * @date 2018/5/27 22:27
 */
public class AsyncMasterImpl implements Watcher{
    private ZooKeeper zooKeeper;
    private String connectString;
    private String serverId = Integer.toHexString(new Random().nextInt());
    private static boolean isLeader = false;

    public AsyncMasterImpl(String connectString){
        this.connectString = connectString;
    }

    void startZk() throws IOException {
        zooKeeper = new ZooKeeper(connectString,2000,this);
    }

    void stopZk() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws Exception {
        AsyncMasterImpl asyncMaster = new AsyncMasterImpl("23.106.132.161:2181");
        asyncMaster.startZk();
        asyncMaster.runForMaster();
        Thread.sleep(6000);
        asyncMaster.stopZk();
    }

    AsyncCallback.StringCallback stringCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String s, Object o, String s1) {
            if (o==null){
                System.out.println("obj parameter is null");
            }
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    isLeader = true;
                    break;
                default:
                    isLeader = false;
            }
            System.out.println("I'm "+(isLeader?"":"not")+"the leader");
        }
    };

    void runForMaster(){
        zooKeeper.create("/master",serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL,
                stringCallback,null);
    }

    AsyncCallback.DataCallback dataCallback = new AsyncCallback.DataCallback(){
        @Override
        public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case NONODE:
                    runForMaster();
                    return;
            }
        }
    };

    void checkMaster(){
        zooKeeper.getData("/master",false,dataCallback,null);
    }

}
