/*
Copyright
All materials provided to the students as part of this course is the property of respective authors. Publishing them to third-party (including websites) is prohibited. Students may save it for their personal use, indefinitely, including personal cloud storage spaces. Further, no assessments published as part of this course may be shared with anyone else. Violators of this copyright infringement may face legal actions in addition to the University disciplinary proceedings.
©2022, Joseph D’Silva
*/
import java.io.*;

import java.util.*;
import java.util.HashMap;

// To get the name of the host.
import java.net.*;
import java.nio.charset.StandardCharsets;

//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;

// TODO
// Replace XX with your group number.
// You may have to add other interfaces such as for threading, etc., as needed.
// This class will contain the logic for both your master process as well as the worker processes.
//  Make sure that the callbacks and watch do not conflict between your master's logic and worker's logic.
//		This is important as both the master and worker may need same kind of callbacks and could result
//			with the same callback functions.
//	For a simple implementation I have written all the code in a single class (including the callbacks).
//		You are free it break it apart into multiple classes, if that is your programming style or helps
//		you manage the code more modularly.
//	REMEMBER !! ZK client library is single thread - Watches & CallBacks should not be used for time consuming tasks.
//		Ideally, Watches & CallBacks should only be used to assign the "work" to a separate thread inside your program.
public class DistProcess implements Watcher
{
	ZooKeeper zk;
	String zkServer, pinfo;
	boolean isMaster=false;
	boolean initalized=false;

	String workerNodeName;
	String workerNodePath;

	//Hashmap for worker node list
	HashMap<String, String> workersList = new HashMap<>();

	//Queue for pending tasks
	Queue<String> pendingTasks = new LinkedList<String>();
	
	//Tasks picked up by master
	ArrayList<String> readTasks = new ArrayList<String>();

	DistProcess(String zkhost)
	{
		zkServer=zkhost;
		pinfo = ManagementFactory.getRuntimeMXBean().getName();
		System.out.println("DISTAPP : ZK Connection information : " + zkServer);
		System.out.println("DISTAPP : Process information : " + pinfo);
	}

	void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException
	{
		zk = new ZooKeeper(zkServer, 10000, this); //connect to ZK.
	}

	void initalize() throws IOException, UnknownHostException, KeeperException, InterruptedException
	{
		try
		{
			runForMaster();	// See if you can become the master (i.e, no other master exists)
			isMaster=true;
			
			System.out.println("DISTAPP : Role : I will be functioning as " + ( isMaster ? "master" : "worker"));

			masterGetWorkers();
			masterGetTasks();

		}catch(NodeExistsException nee)
		{ 
			isMaster=false; 

			System.out.println("DISTAPP : Role : I will be functioning as " + ( isMaster ? "master" : "worker"));
			
			// Create worker node
			workerNodePath = zk.create("/dist10/workers/worker-", "idle".getBytes(), Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
			workerNodeName = workerNodePath.replace("/dist10/workers/", "");
			
			// Get task
			workerGetTask(workerNodeName);
		
		} 
		catch(UnknownHostException uhe)
		{ System.out.println(uhe); }
		catch(KeeperException ke)
		{ System.out.println(ke); }
		catch(InterruptedException ie)
		{ System.out.println(ie); }

	}

	//Master fetching worker nodes
	void masterGetWorkers()
	{
		// asynchronous call with a Watcher and a Callback
		zk.getChildren("/dist10/workers", masterWorkerWatcher, masterWorkerCallback, null);  
	}

	//Master fetching task znodes...
	void masterGetTasks()
	{
		zk.getChildren("/dist10/tasks", masterTaskWatcher, masterTaskCallback, null);
	}

	void workerGetTask(String workerNodeName)
	{
		// asynchronous call with a Watcher and a Callback
		zk.getData("/dist10/workers/" + workerNodeName, workerTaskWatcher, workerTaskCallback, null);  
	}

	// Try to become the master.
	void runForMaster() throws UnknownHostException, KeeperException, InterruptedException
	{
		//Try to create an ephemeral node to be the master, put the hostname and pid of this process as the data.
		// This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
		zk.create("/dist10/master", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	//-------Watchers-------//

	// Master looking after workers
	Watcher masterWorkerWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			System.out.println("DISTAPP : Event received : " + e);
			
			if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist10/workers"))
			{
				// There has been changes to the children of the workers node.
				// We are going to re-install the Watch as well as request for the list of the children
				masterGetWorkers();
			}
		}
	};

	// Master looking after tasks
	Watcher masterTaskWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			System.out.println("DISTAPP : Event received : " + e);
			if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist10/tasks"))
			{
				// There has been changes to the children of the tasks node.
				// We are going to re-install the Watch as well as request for the list of the children or the list of if tasks.
				masterGetTasks();
			}
		}
	};

	// Worker looking for tasks
	Watcher workerTaskWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			System.out.println("DISTAPP : Event received : " + e);
			if(e.getType() == Watcher.Event.EventType.NodeDataChanged && e.getPath().equals(workerNodePath))
			{
				workerGetTask(workerNodeName);
			}
		}
	};

	// Master checking after tasks are completed
	Watcher taskCompletedWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			System.out.println("DISTAPP : Event received : " + e);
			if(e.getType() == Watcher.Event.EventType.NodeDataChanged)
			{
				String workerName = e.getPath().replace("/dist10/workers/", "");
				zk.getData("/dist10/workers/" + workerName, taskCompletedWatcher, taskCompletedCallback, null);
			}
		}
	};



	//-------Callbacks------//

	// Callback for Master to Worker nodes
	AsyncCallback.ChildrenCallback masterWorkerCallback = new AsyncCallback.ChildrenCallback() {
		public void processResult(int rc, String path, Object ctx, List<String> children) {
			System.out.println("DISTAPP : masterWorkerCallback : processResult : ChildrenCallback : " + rc + ":" + path + ":" + ctx);
			
			for (String c: children) 
			{
				try
				{ 
					// Check Hashmap if there is a new worker
					boolean newWorker = !(workersList.containsKey(c));
					
					if (newWorker) {
						
						// Add the new worker to the hashmap and set the status of worker to "idle"
						workersList.put(c, "idle");
						
						// Check if there are any pending tasks in the queue
						if (pendingTasks.size() > 0) {
							
							// Get topmost pending task and dequeue it
							String nextTask = pendingTasks.poll();
							
							// Set the status of the worker to the "task name" in the HashMap
							workersList.replace(c,nextTask);		
							
							// Set the DATA of the worker node to the "task name"
							zk.setData("/dist10/workers/" + c, nextTask.getBytes(), -1);
							
							// Need to keep watcher on the DATA of the worker node in case the worker finishes the task
							zk.getData("/dist10/workers/" + c, taskCompletedWatcher, taskCompletedCallback, null);	
						}
					}
					else {
						// If it not a new worker (We do not have to worry about this scenario)
							// do nothing
					}
				}
				catch(NodeExistsException nee){System.out.println(nee);}
				catch(KeeperException ke){System.out.println(ke);}
				catch(InterruptedException ie){System.out.println(ie);}
			}
		}
	};

	// Method to check Idle Workers in HashMap
	public String getIdleWorker(Map<String, String> map, String value) {

		Set<String> result = new HashSet<>();
		
		for (Map.Entry<String, String> entry : map.entrySet()) {
			if (Objects.equals(entry.getValue(), value)) {
				result.add(entry.getKey());
				return entry.getKey();
			}
		}

		return null;
	}


	// Callback for Master to check on tasks
	AsyncCallback.ChildrenCallback masterTaskCallback = new AsyncCallback.ChildrenCallback() {
		public void processResult(int rc, String path, Object ctx, List<String> children) {

			System.out.println("DISTAPP : masterTaskCallback : processResult : ChildrenCallback : " + rc + ":" + path + ":" + ctx);
			
			for (String taskName: children) 
			{
				try 
				{
					// Check data structure to see if it is a new task
					boolean notNewTask = readTasks.contains(taskName);
					
					if (!notNewTask) {
						
						readTasks.add(taskName);

						//Add the task to end of the queue
						pendingTasks.offer(taskName);

						// Check Hashmap if there is an idle worker
						boolean idleWorkerExists = workersList.containsValue("idle");
						
						if (idleWorkerExists){
							
							// Get the name of the idle worker
							String idleWorkerName = getIdleWorker(workersList, "idle");
							
							// Get topmost pending task and dequeue it
							String nextTask = pendingTasks.poll();
							
							// Set the status of the worker to the "task name" in the HashMap
							workersList.replace(idleWorkerName,nextTask);
							
							// Set the DATA of the worker node to the "task name"
							zk.setData("/dist10/workers/" + idleWorkerName, nextTask.getBytes(), -1);
							
							// Need to keep watcher on the DATA of the worker node in case the worker finishes the task
							zk.getData("/dist10/workers/" + idleWorkerName, taskCompletedWatcher, taskCompletedCallback, null);
						}
					}
					else {
						// do nothing
					}
				}
				catch(NodeExistsException nee){System.out.println(nee);}
				catch(KeeperException ke){System.out.println(ke);}
				catch(InterruptedException ie){System.out.println(ie);}
				
				
			}	
		}
	};

	// Callback for worker to check on tasks and execute them
	AsyncCallback.DataCallback workerTaskCallback = new AsyncCallback.DataCallback() {
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			
			System.out.println("DISTAPP : workerTaskCallback : processResult : DataCallback : " + rc + ":" + path + ":" + ctx + ":" + stat);

			// Check the Data of the worker node to see if the worker has been assigned a task
			String workerStatus = new String(data, StandardCharsets.UTF_8);
			boolean taskAssigned = !("idle".equals(workerStatus));
			
			if (taskAssigned) 
			{
				// Create a new thread on which the task is completed
				Thread execute_task = new Thread (() -> {
					try 
					{
						byte[] taskSerial = zk.getData("/dist10/tasks/" + workerStatus, false, null);
						
						// Re-construct our task object.
						ByteArrayInputStream bis_task = new ByteArrayInputStream(taskSerial);
						ObjectInput in_task = new ObjectInputStream(bis_task);
						DistTask dt = (DistTask) in_task.readObject();
						
						//Execute the task
						dt.compute();
						
						// Serialize our Task object back to a byte array!
						ByteArrayOutputStream bos = new ByteArrayOutputStream();
						ObjectOutputStream oos = new ObjectOutputStream(bos);
						oos.writeObject(dt); oos.flush();
						taskSerial = bos.toByteArray();
						
						// Store it inside the result node.
						zk.create("/dist10/tasks/"+ workerStatus +"/result", taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						
						// Set the data of the worker node to "idle" so that the master knows that the worker is done with the task.
						zk.setData("/dist10/workers/" + workerNodeName , "idle".getBytes(), -1);
					}
					catch(NodeExistsException nee){System.out.println(nee);}
					catch(KeeperException ke){System.out.println("!@#$");System.out.println(ke);}
					catch(InterruptedException ie){System.out.println(ie);}
					catch(IOException io){System.out.println(io);}
					catch(ClassNotFoundException cne){System.out.println(cne);}
				});	// End of the thread
				
				// Run the thread
				execute_task.start();
			}
			else {
				// do nothing, a watcher has already been set
			}
		}
	};

	// Callback for completed tasks
	AsyncCallback.DataCallback taskCompletedCallback = new AsyncCallback.DataCallback() {
		public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
			
			System.out.println("DISTAPP : taskCompletedCallback: processResult : DataCallback : " + rc + ":" + path + ":" + ctx + ":" + stat);
			
			try 
			{
				//  Check if the DATA of the worker node is "idle"
				String workerStatus = new String(data, StandardCharsets.UTF_8);
				boolean taskComplete = "idle".equals(workerStatus);
				
				// If it is idle, this means that the worker has completed the task
				if (taskComplete) {
					
					// Get the name of the worker node
					String workerName = path.replace("/dist10/workers/", "");
					
					// Set the status of the worker to "idle" in the HashMap
					workersList.replace(workerName,"idle");
					
					// Check to see if there is a pending task
					// If there is a pending task
					if (pendingTasks.size() > 0) {
						
						// Get topmost pending task and dequeue it
						String nextTask = pendingTasks.poll();
						
						// Set the status of the worker to the "task name" in the HashMap
						workersList.replace(workerName,nextTask);	
						
						// Set the DATA of the worker node to the "task name"
						zk.setData("/dist10/workers/" + workerName, nextTask.getBytes(), -1);
						
						// Need to keep watcher on the DATA of the worker node in case the worker finishes the task
						zk.getData("/dist10/workers/" + workerName, taskCompletedWatcher, taskCompletedCallback, null);
					}
				}
				else 
				{
					// Do nothing, a watcher has been set up already	
				}
			}
			catch(NodeExistsException nee){System.out.println(nee);}
			catch(KeeperException ke){System.out.println(ke);}
			catch(InterruptedException ie){System.out.println(ie);}
		}
	};

	// Watcher instantiation when zookeeper object is instantiated
	public void process(WatchedEvent e)
	{
		//Get watcher notifications.

		//!! IMPORTANT !!
		// Do not perform any time consuming/waiting steps here
		//	including in other functions called from here.
		// 	Your will be essentially holding up ZK client library 
		//	thread and you will not get other notifications.
		//	Instead include another thread in your program logic that
		//   does the time consuming "work" and notify that thread from here.

		System.out.println("DISTAPP : Event received : " + e);

		if(e.getType() == Watcher.Event.EventType.None) // This seems to be the event type associated with connections.
		{
			// Once we are connected, do our intialization stuff.
			if(e.getPath() == null && e.getState() ==  Watcher.Event.KeeperState.SyncConnected && initalized == false) 
			{
				try{
					initalize();
					initalized = true;
				}
				catch(IOException io)
				{System.out.println(io);}
				catch(KeeperException ke)
				{ System.out.println(ke); }
				catch(InterruptedException ie)
				{ System.out.println(ie); }
				
				
			}
		}

	}



	public static void main(String args[]) throws Exception
	{
		//Create a new process
		//Read the ZooKeeper ensemble information from the environment variable.
		DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
		dt.startProcess();

		//Replace this with an approach that will make sure that the process is up and running forever.
		//Thread.sleep(20000); 

		while(true){
			//do nothing - keep on looping
		}

	}
}
