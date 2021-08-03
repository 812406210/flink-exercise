package server.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.EOFException;
import java.util.concurrent.TimeUnit;


/**
 * @program: maxwell
 * @description:
 * @author: yang
 * @create: 2021-08-01 22:12
 */
public class LeaderLatchTest {
	static String LOCK_PATH = "/test/leader_latch";

	public static void main(String[] args) throws Exception{
		CuratorFramework client = getZkClient();
		//LeaderLatch leaderLatch = new LeaderLatch(client, LOCK_PATH, "CLIENT_TEST");
		LeaderLatch leaderLatch = new LeaderLatch(client, LOCK_PATH);

		ZkJobLeaderLatchListener listener =  new ZkJobLeaderLatchListener();
		leaderLatch.addListener(listener);

		leaderLatch.start();
		awaitByLeaderLatch(leaderLatch);

		Thread.sleep(8000);


		if (leaderLatch.hasLeadership()) {
			System.out.println("------close----------begin----");
			leaderLatch.close();
			System.out.println("------close----------end----");
		}

		Thread.sleep(5000);
	}

	/*
	 *   阻塞直到获得领导权
	 * */
	public static void awaitByLeaderLatch(LeaderLatch leaderLatch) {
		try {
			leaderLatch.await();
		} catch (InterruptedException | EOFException e) {
			e.printStackTrace();
		}
	}

	/*
	 *   尝试获得领导权并超时
	 * */
	public static boolean awaitByLeaderLatch(LeaderLatch leaderLatch,long timeout, TimeUnit unit) {
		boolean hasLeadership = false;
		try {
			hasLeadership = leaderLatch.await(timeout, unit);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return  hasLeadership;
	}

	private static CuratorFramework getZkClient() {
		String zkServerAddress = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
		ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3, 5000);
		CuratorFramework zkClient = CuratorFrameworkFactory.builder()
				.connectString(zkServerAddress)
				.sessionTimeoutMs(5000)
				.connectionTimeoutMs(5000)
				.retryPolicy(retryPolicy)
				.build();
		zkClient.start();
		return zkClient;
	}

	static class ZkJobLeaderLatchListener implements LeaderLatchListener {

		@Override
		public void isLeader() {
			System.out.println("成为leader:");
		}

		@Override
		public void notLeader() {
			System.out.println("不是leader:");
		}

	}

}

