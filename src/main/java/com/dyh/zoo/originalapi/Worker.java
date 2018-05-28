package com.dyh.zoo.originalapi;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * @author bang
 * @date 2018/5/28 20:55
 */
public class Worker implements Watcher{

    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    private String hostPort;
    private ZooKeeper zooKeeper;
    String serverId = Integer.toHexString(new Random().nextInt());

    private String status;

    public Worker(String hostPort){
        this.hostPort = hostPort;
    }

    void startZk() throws IOException {
        zooKeeper = new ZooKeeper(hostPort,15000,this);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        logger.info(watchedEvent.toString()+","+hostPort);
    }

    void register(){
        zooKeeper.create("/workers/worker-"+serverId,"Idle".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,createWorkerCallBack,null);
    }

    AsyncCallback.StringCallback createWorkerCallBack = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String path, Object o, String s1) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    logger.info("Registered successfully:"+serverId);
                    break;
                case NODEEXISTS:
                    logger.warn("Already registered:"+serverId);
                    break;
                default:
                    logger.error("Something went wrong:",KeeperException.create(KeeperException.Code.get(i),path));
            }
        }
    };

    AsyncCallback.StatCallback statusUpdateCallBack = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    updateStatus((String)ctx);
            }
        }
    };

    private synchronized void updateStatus(String status){
        if (status == this.status){
            // -1表示禁止版本号检查，进行无条件更新
            zooKeeper.setData("/workers/worker-"+serverId,status.getBytes(),-1,statusUpdateCallBack,status);
        }
    }

    public void setStatus(String status){
        // 将状态信息保存到本地，方便更新失败后的重试
        this.status = status;
        updateStatus(status);
    }

    public static void main(String[] args) throws Exception{
        Worker worker = new Worker("23.106.132.161:2181,23.106.132.161:2182,23.106.132.161:2183");
        worker.startZk();
        worker.register();
        Thread.sleep(60000);
    }

}
