package server.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @program: maxwell
 * @description:
 * @author: yang
 * @create: 2021-08-02 10:52
 */
public class LatchDemo {
	private static CuratorFramework client  = CuratorFrameworkFactory.newClient("hadoop101:2181,hadoop102:2181,hadoop103:2181", new ExponentialBackoffRetry(3000, 3));
	private static String           path    = "/test1/master";
	private static String           id = "0001";

	public static void main(String[] args) throws Exception {
		// curator客户端启动
		client.start();
		// 创建选举实例
		LeaderLatch latch = new LeaderLatch(client, path, id);
		// 添加选举监听
		latch.addListener(new LeaderLatchListener() {
			@Override
			public void isLeader() {
				// 如果成为master则触发
				System.out.println("is leader");
			}

			@Override
			public void notLeader() {
				// 如果从主节点变成非主节点则触发
				System.out.println("not leader");
			}
		});
		// 加入选举
		latch.start();
		Thread.sleep(8000);
		// curator客户端关闭
		client.close();
	}
}
