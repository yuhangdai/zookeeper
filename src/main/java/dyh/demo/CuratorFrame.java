package dyh.demo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;

/**
 * Created by bang on 2018/5/27.
 */
public class CuratorFrame {
    public static void main(String[] args) throws Exception {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder().build();
        curatorFramework.create().forPath("/testaaa","abcd".getBytes());
    }
}
