package com.dyh.zoo.originalapi;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * zookeeper watcher实现,用于监控zookeeper客户端连接
 * @author bang
 * @date 2018/5/27 17:57
 */
public class MasterImpl implements Watcher{

    private static final Logger logger = LoggerFactory.getLogger(MasterImpl.class);

    private ZooKeeper zooKeeper;
    private String connectString;
    String serverId = Integer.toHexString(new Random().nextInt());
    static boolean isLeader = false;

    public MasterImpl(String connectString){
        this.connectString = connectString;
    }

    public void startZk() throws IOException {
        zooKeeper = new ZooKeeper(connectString,30000,this);
    }

    public void stopZK() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws Exception {
        MasterImpl watcher = new MasterImpl("23.106.132.161:2181");
        watcher.startZk();
        watcher.bootstrap();

        // 竞争成为主节点
        watcher.runForMaster();
        if (isLeader){
            System.out.println("i am the leader");
            Thread.sleep(120000);
        } else {
            System.out.println("Someone else is the leader");
        }

        watcher.stopZK();
    }


    void runForMaster() throws InterruptedException {
        while (true){
            try{
                zooKeeper.create("/master",serverId.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (KeeperException.NodeExistsException e){
                isLeader = false;
                break;
            } catch (KeeperException.ConnectionLossException e){
                // create 方法执行且发生了ConnectionLoss，此时无法判断是否为master 因此需要checkMaster
            } catch (KeeperException e){

            }
            if (checkMaster()) break;
        }

    }

    /**
     * returns true if there is a master
     */
    boolean checkMaster() throws InterruptedException{
        while (true){
            try{
                Stat stat = new Stat();
                byte[] data = zooKeeper.getData("/master",false,stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (KeeperException.NoNodeException e){
                return false;
            } catch (KeeperException.ConnectionLossException e){

            } catch (KeeperException e){

            }
        }
    }

    void bootstrap(){
        createParent("/workers",new byte[0]);
        createParent("/assign",new byte[0]);
        createParent("/tasks",new byte[0]);
        createParent("/status",new byte[0]);
    }

    /**
     * 创建znode
     */
    void createParent(String path,byte[] data){
        zooKeeper.create(path,data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,createParentCallBack,data);
    }

    AsyncCallback.StringCallback createParentCallBack = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String path, Object ctx, String s1) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    createParent(path,(byte[])ctx);
                    break;
                case OK:
                    logger.info("Parent created");
                    break;
                case NODEEXISTS:
                    logger.warn("Parent already registered:"+path);
                    break;
                default:
                    logger.error("Something went wrong:",KeeperException.create(KeeperException.Code.get(i),path));
            }
        }
    };

}
