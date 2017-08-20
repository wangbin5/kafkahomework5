package com.test.kafka.lesson3;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
public class ZookeeperLeader implements Watcher {
	
	interface LeaderStatsListner {
		public void change(boolean isLeader);
	}
	
	public static final String path = "/kafka";
	public static final String leaderPath = path+"/leader";
	public static final Watcher EMPTY = new Watcher(){
		@Override
		public void process(WatchedEvent event) {
		}
	};
	private LeaderStatsListner listener;
	private boolean leader;
	private ZooKeeper zk;
	String currentId;
	public void init() throws IOException{
		zk = new ZooKeeper("192.168.231.129:2181", 3000, EMPTY);
	}
	
	public boolean toLeader() throws InterruptedException, ExecutionException{
		final CompletableFuture<Boolean> future = new CompletableFuture<>();		 
		this.listener = new LeaderStatsListner(){
			@Override
			public void change(boolean isLeader) {
				if(isLeader){
					future.complete(true);
				}
				
			}
			
		};
		run();
		return future.get();
	}
	
	public boolean getResultNoWait(LeaderStatsListner listener){
		this.listener = listener;
		run();
		return this.leader;
	}
	
	public void run() {
		System.out.println("in run method ");
		try{
			//第一步： 判断节点是否存在，不存在则创建结点
			if(currentId==null || zk.exists(currentId,EMPTY)==null){				
				currentId= zk.create(leaderPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);				
			}
			//第二步： 检查节点，判断序号是否最小，如果最小，设置为leader
			List<String> ids = zk.getChildren(path, false);
			if(isMin(ids,currentId)){
				this.leader = true;
				if(listener!=null){
					listener.change(this.leader);
				}
			}
			else{
				//第三部： 注册监听函数到上一个节点
				String lastId = getLastId(ids,currentId);
				zk.exists(path+"/"+lastId, new Watcher(){	
					@Override
					public void process(WatchedEvent event) {
						run();
						
					}
				});
			}			
			
		}
		catch(KeeperException | InterruptedException ex){
			this.leader = false;
			this.currentId = null;
			if(listener!=null){
				listener.change(this.leader);
			}
		}
		
	
	}
	
	
	private String getLastId(List<String> ids, String currentId) {
		String lastId = null;
		for(String id : ids ){
			if(less(id,currentId)){
				if(lastId == null){
					lastId = id;
				}
				else if(!less(id,lastId)){
					lastId = id;
				}
			}
		}
		return lastId;
	}
	private boolean isMin(List<String> ids, String currentId) {
		System.out.println("check min "+currentId+",all "+ toString(ids));
		for(String id  : ids ){
			if(less(id,currentId)){
				return false;
			}
		}
		return true;
	}
	 
	private String toString(List<String> ids) {
		StringBuilder builder = new StringBuilder();
		for(String id : ids ){
			builder.append(id).append(",");
		}
		return builder.toString();
	}

	private boolean less(String id1, String id2) {
		long number1 = toNumber(id1);
		long number2 = toNumber(id2);
		return number1 < number2;
	}

	private long toNumber(String id) {
		StringBuilder builder = new StringBuilder();
		for(int i = id.length();i>0;i--){
			char ch = id.charAt(i-1);
			if(Character.isDigit(ch)){
				builder.insert(0, ch);
			}
		}
		return Long.parseLong(builder.toString());
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println("event "+event.getType()+","+event.getPath());
		
	}
	
	public static void main(String[] args) throws KeeperException, InterruptedException, IOException, ExecutionException{
		ZookeeperLeader leader = new ZookeeperLeader();
		leader.init();
		Thread.sleep(new Random().nextInt(3000));
		boolean isLeader = leader.toLeader();
		System.out.println("current node is leader :"+isLeader);
	}
}
