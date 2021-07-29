package server;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @program: FlinkSql
 * @description:
 * @author: yang
 * @create: 2021-05-26 14:00
 */
public class ZookeeperContext {

    protected static Logger logger = LoggerFactory.getLogger("zk");

    public final int SESSION_TIME_OUT = 2000;
    public ZooKeeper zk;
    /**
     * 判断zk的链接链接状态
     * */
    public boolean isConnected(){
        return zk.getState() == ZooKeeper.States.CONNECTED;
    }

    /**
     * 创建zk链接
     * **/
    public ZookeeperContext(String connectString) {
        try {
            /**
             * connectString 链接zookeeper的Ip和端口，多个用逗号隔开例如：
             * 10.0.0.104:2181,10.0.0.105:2181 sessionTimeout
             * 客户端和zookeeper链接断开后，数据最长保存的时间 watcher 监控回调，服务器数据修改会回调
             */
            zk = new ZooKeeper(connectString, SESSION_TIME_OUT, new ZookeeperWatcher());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * zk监听，如果服务器端有什么变化到这里接收
     * 并继续添加监听
     * */
    public class ZookeeperWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            try {

                if(null != event.getPath()){
                    System.out.println( event );
                    if(event.getType() == EventType.NodeChildrenChanged){
                        zk.getChildren(event.getPath(), true) ;
                    }else{
                        zk.getData(event.getPath(), true, null) ;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 在zk创建数据
     * */
    public void setData(String path,byte[]data , CreateMode createMode){
        try {
            if(zk.exists(path, false) == null){
                zk.create(path, data, Ids.OPEN_ACL_UNSAFE, createMode) ;
            }else{
                zk.setData(path,data,-1);
            }
            zk.getChildren(path, true) ;
        } catch (Exception e) {
            logger.error("setData", e );
        }
    }

    public static void main(String[] args)throws Exception {
        ZookeeperContext context = new ZookeeperContext("10.0.0.104:2181");

        while (!context.isConnected()) {
            Thread.sleep(3000);
        }
        context.setData("/root", "1".getBytes(),CreateMode.PERSISTENT);
        context.setData("/root/hpgary", "gg".getBytes() , CreateMode.EPHEMERAL);
        System.err.println("eee");
        System.in.read() ;
    }

}