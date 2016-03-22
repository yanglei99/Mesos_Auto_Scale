/*******************************************************************************
* Copyright (c) 2016 IBM Corp.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*******************************************************************************/

/**
 * reference https://github.com/codefutures/mesos-docker-tutorial/blob/master/src/main/java/com/codefutures/tutorial/mesos/docker/ExampleScheduler.java
 * and https://github.com/Netflix/Fenzo/blob/master/fenzo-core/src/main/java/com/netflix/fenzo/samples/SampleFramework.java
 */
package com.mesos.framework.fenzo;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mesos.framework.AutoScaleCluster;
import com.mesos.framework.Constants;
import com.mesos.framework.TaskMonitor;
import com.mesos.framework.impl.MesosMonitor;
import com.mesos.framework.softlayer.SoftlayerAgent;
import com.netflix.fenzo.AutoScaleAction;
import com.netflix.fenzo.AutoScaleRule;
import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.ScaleDownAction;
import com.netflix.fenzo.ScaleUpAction;
import com.netflix.fenzo.SchedulingResult;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VMAssignmentResult;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.functions.Func1;
import com.netflix.fenzo.plugins.ExclusiveHostConstraint;
import com.netflix.fenzo.plugins.HostAttrValueConstraint;
import com.netflix.fenzo.plugins.VMLeaseObject;
import com.netflix.fenzo.sla.ResAllocs;
import com.netflix.fenzo.sla.ResAllocsBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Example scheduler to launch Docker containers. */
public class FenzoScheduler implements Scheduler, Constants {

  /** Logger. */
  private static final Logger logger = LoggerFactory.getLogger(FenzoScheduler.class);

  private static final String FENZO_KEY_PREFIX = KEY_PREFIX+"fenzo.";
  private static final String TASK_CPU_KEY = FENZO_KEY_PREFIX+"cpu";
  private static final String TASK_MEM_KEY = FENZO_KEY_PREFIX+"memory";
  private static final String TASK_DISK_KEY = FENZO_KEY_PREFIX+"disk";
  private static final String TASK_NET_KEY = FENZO_KEY_PREFIX+"net";

  private static final String TASK_MAX_CPU_KEY = FENZO_KEY_PREFIX+"max.cpu";
  private static final String TASK_MAX_MEM_KEY = FENZO_KEY_PREFIX+"max.memory";
  private static final String TASK_MAX_DISK_KEY = FENZO_KEY_PREFIX+"max.disk";
  private static final String TASK_MAX_NET_KEY = FENZO_KEY_PREFIX+"max.net";
  
  private static final float TASK_CPU_UNDEFINED = 1;
  private static final float TASK_MEM_UNDEFINED = 1024;
  private static final float TASK_DISK_UNDEFINED = 10;
  private static final float TASK_NET_UNDEFINED = 0;

  private static final String TASK_MAX_IDLE_KEY = FENZO_KEY_PREFIX+"max.idle";
  private static final String TASK_MIN_IDLE_KEY = FENZO_KEY_PREFIX+"min.idle";
  
  private static final int TASK_MIN_IDLE_UNDEFINED = 1;
  private static final int TASK_MAX_IDLE_UNDEFINED = 2;

  private static final String TASK_LEASE_EXPIRE_KEY = FENZO_KEY_PREFIX+"lease.expire";
  private static final Long TASK_LEASE_EXPIRE_UNDEFINED = 10L;

  private static final String TASK_SCALE_WAIT_OFFER_KEY = FENZO_KEY_PREFIX+"scale.wait.offer";
  private static final Long TASK_SCALE_WAIT_OFFER_UNDEFINED = 5L;

  private static final String TASK_SCALE_UP_DELAY_KEY = FENZO_KEY_PREFIX+"scale.up.delay";
  private static final Long TASK_SCALE_UP_DELAY_UNDEFINED = 6L;

  private static final String TASK_SCALE_DOWN_DELAY_KEY = FENZO_KEY_PREFIX+"scale.down.delay";
  private static final Long TASK_SCALE_DOWN_DELAY_UNDEFINED = 10L;

  private static final String TASK_SCALE_INTERVAL_KEY = FENZO_KEY_PREFIX+"scale.interval";
  private static final Long TASK_SCALE_INTERVAL_UNDEFINED = 60L;

  private static final String TASK_SCHEDULE_DELAY_KEY = FENZO_KEY_PREFIX+"schedule.delay";
  private static final Long TASK_SCHEDULE_DELAY_UNDEFINED = 1L;


  private static final String TASK_HOST_ATTRIBUTE_KEY = FENZO_KEY_PREFIX+"host.attribute";
  private static final String TASK_HOST_ATTRIBUTE_NAME_NOT_DEFINED = "*";
  private static final String TASK_HOST_ATTRIBUTE_VALUE_NOT_DEFINED = "*";


  /** Docker image name e..g. "acemair". */
  private final String frameworkName;

  private final String taskName;

  /** Docker image name e..g. "acemair". */
  private final String imageName;

  /** Number of instances to run, the minimum. */
  private final float taskCPU;
  private final float taskMEM;
  private final float taskDISK;
  private final float taskNET;

  private final int taskMinIdle;
  private final int taskMaxIdle;
  
  private final String[] hostAttribute;

  private final Long leaseExpireSec;
  private final Long scaleWaitNewOfferSec;
  private final Long delayAutoscaleUpBySecs;
  private final Long delayAutoscaleDownBySecs;
  private final Long scaleIntervalBySecs;
  private final long scheduleDelayTS;

  /** The environment variables */
  private final HashMap<String, String> envMap;
  
  /** List of pending instances. */
  private final List<String> pendingInstances = new ArrayList<>();

  /** List of running instances. */
  private final List<String> runningInstances = new ArrayList<>();

  /** Fenzo Scheduler */
  private final TaskScheduler fenzoScheduler;

  /** MesosSchedulerDriver Ref */
  private final AtomicReference<MesosSchedulerDriver>  mesosSchedulerDriverRef;
  
  /** Fenzo Launched Task Host Map */
  private final Map<String, String> launchedTaskHostMap = new HashMap<String, String>();

  /** Fenzo ResourceOffer Lease Queue */
  private final BlockingQueue<VirtualMachineLease> leasesQueue = new LinkedBlockingQueue<>();;

  /** Fenzo Pending Task Map */
  private final Map<String, TaskRequest> pendingTasksMap = new HashMap<>();

  /** Fenzo Task Queue */
  BlockingQueue<TaskRequest> taskQueue = new LinkedBlockingQueue<>();
  
  /** Fenzo Check Framework shutdown Queue */
  
  private final AtomicBoolean isShutdown ;
  
  private final AtomicReference<Long> lastDeclineOfferInSec;
  
  private final AtomicReference<Long> lastScaleInSec;

  private final AutoScaleCluster scaleClusterAgent ; // TODO read from config later

  // For Scale Task
  
  private final TaskMonitor monitorAgent ; // TODO read from config later

  /** Constructor. */
  public FenzoScheduler(AtomicBoolean isShutdown, AtomicReference<MesosSchedulerDriver> ref,String frameworkName, String imageName, int desiredInstances, HashMap<String, String> envMap) {
	this.frameworkName = frameworkName;
	this.taskName = "task-"+frameworkName;
    this.imageName = imageName;
    this.envMap= envMap;
    this.mesosSchedulerDriverRef=ref;
    this.isShutdown = isShutdown;
    
    ResAllocsBuilder resAllocsBuilder = new ResAllocsBuilder(frameworkName);
    if (envMap.containsKey(TASK_MAX_CPU_KEY)) resAllocsBuilder.withCores(new Float(envMap.get(TASK_MAX_CPU_KEY)));
    if (envMap.containsKey(TASK_MAX_MEM_KEY)) resAllocsBuilder.withMemory(new Float(envMap.get(TASK_MAX_MEM_KEY)));
    if (envMap.containsKey(TASK_MAX_DISK_KEY)) resAllocsBuilder.withDisk(new Float(envMap.get(TASK_MAX_DISK_KEY)));
    if (envMap.containsKey(TASK_MAX_NET_KEY)) resAllocsBuilder.withNetworkMbps(new Float(envMap.get(TASK_MAX_NET_KEY)));
    ResAllocs resAllocs = resAllocsBuilder.build();
    Map<String, ResAllocs> allResAllocs = new HashMap<String,ResAllocs>();
    allResAllocs.put(frameworkName, resAllocs);
    
    this.taskCPU =  (envMap.containsKey(TASK_CPU_KEY)) ? (new Float(envMap.get(TASK_CPU_KEY))): TASK_CPU_UNDEFINED;
    this.taskMEM =  (envMap.containsKey(TASK_MEM_KEY)) ? (new Float(envMap.get(TASK_MEM_KEY))): TASK_MEM_UNDEFINED;
    this.taskDISK =  (envMap.containsKey(TASK_DISK_KEY)) ? (new Float(envMap.get(TASK_DISK_KEY))): TASK_DISK_UNDEFINED;
    this.taskNET =  (envMap.containsKey(TASK_NET_KEY)) ? (new Float(envMap.get(TASK_NET_KEY))): TASK_NET_UNDEFINED;

    this.taskMinIdle =  (envMap.containsKey(TASK_MIN_IDLE_KEY)) ? (new Integer(envMap.get(TASK_MIN_IDLE_KEY))): TASK_MIN_IDLE_UNDEFINED;
    this.taskMaxIdle =  (envMap.containsKey(TASK_MAX_IDLE_KEY)) ? (new Integer(envMap.get(TASK_MAX_IDLE_KEY))): TASK_MAX_IDLE_UNDEFINED;

    this.leaseExpireSec = (envMap.containsKey(TASK_LEASE_EXPIRE_KEY)) ? (new Long(envMap.get(TASK_LEASE_EXPIRE_KEY))): TASK_LEASE_EXPIRE_UNDEFINED;
    this.scaleWaitNewOfferSec = (envMap.containsKey(TASK_SCALE_WAIT_OFFER_KEY)) ? (new Long(envMap.get(TASK_SCALE_WAIT_OFFER_KEY))): TASK_SCALE_WAIT_OFFER_UNDEFINED;
    this.delayAutoscaleUpBySecs = (envMap.containsKey(TASK_SCALE_UP_DELAY_KEY)) ? (new Long(envMap.get(TASK_SCALE_UP_DELAY_KEY))): TASK_SCALE_UP_DELAY_UNDEFINED;
    this.delayAutoscaleDownBySecs = (envMap.containsKey(TASK_SCALE_DOWN_DELAY_KEY)) ? (new Long(envMap.get(TASK_SCALE_DOWN_DELAY_KEY))): TASK_SCALE_DOWN_DELAY_UNDEFINED;
    this.scaleIntervalBySecs = (envMap.containsKey(TASK_SCALE_INTERVAL_KEY)) ? (new Long(envMap.get(TASK_SCALE_INTERVAL_KEY))): TASK_SCALE_INTERVAL_UNDEFINED;
    this.scheduleDelayTS = ((envMap.containsKey(TASK_SCHEDULE_DELAY_KEY)) ? (new Long(envMap.get(TASK_SCHEDULE_DELAY_KEY))): TASK_SCHEDULE_DELAY_UNDEFINED)*1000;

    this.hostAttribute =( (envMap.containsKey(TASK_HOST_ATTRIBUTE_KEY)) ? envMap.get(TASK_HOST_ATTRIBUTE_KEY): TASK_HOST_ATTRIBUTE_NAME_NOT_DEFINED+":"+TASK_HOST_ATTRIBUTE_VALUE_NOT_DEFINED).split(":");

    this.lastDeclineOfferInSec = new AtomicReference<Long>(getCurrentSec());
    this.lastScaleInSec = new AtomicReference<Long>(getCurrentSec());

    this.scaleClusterAgent = hostAttribute[0].equals(TASK_HOST_ATTRIBUTE_NAME_NOT_DEFINED) ? null: new SoftlayerAgent(envMap);
    this.monitorAgent = new MesosMonitor(envMap, desiredInstances, this.launchedTaskHostMap);
    
    TaskScheduler.Builder builder = new TaskScheduler.Builder()
            .withLeaseOfferExpirySecs(leaseExpireSec)
            .withRejectAllExpiredOffers()
            .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                @Override
                public void call(VirtualMachineLease lease) {
                    logger.info("Declining offer on " + lease.hostname());
                    ref.get().declineOffer(lease.getOffer().getId());
                    lastDeclineOfferInSec.set(getCurrentSec());
                }
            })
            .withInitialResAllocs(allResAllocs);
    if (!hostAttribute[0].equals(TASK_HOST_ATTRIBUTE_NAME_NOT_DEFINED))
    {
            builder
            .withAutoScaleByAttributeName(hostAttribute[0])
            .withDelayAutoscaleUpBySecs(delayAutoscaleUpBySecs) 
            .withDelayAutoscaleDownBySecs(delayAutoscaleDownBySecs)
            .withAutoScaleRule(new AutoScaleRule() {
				
				@Override
				public boolean idleMachineTooSmall(VirtualMachineLease arg0) {
					// TODO Auto-generated method stub
					logger.info("cpuCores={}, memoryMB={}, diskMB={}, networkMbps={} ",arg0.cpuCores(),arg0.memoryMB(),arg0.diskMB(),arg0.networkMbps());
					return arg0.cpuCores() < taskCPU || arg0.memoryMB()< taskMEM || arg0.diskMB()< taskDISK || arg0.networkMbps() < taskNET;
				}
				
				@Override
				public String getRuleName() {
					// TODO Auto-generated method stub
					return hostAttribute[1];
				}
				
				@Override
				public int getMinIdleHostsToKeep() {
					// TODO Auto-generated method stub
					return taskMinIdle;
				}
				
				@Override
				public int getMaxIdleHostsToKeep() {
					// TODO Auto-generated method stub
					return taskMaxIdle;
				}
				
				@Override
				public long getCoolDownSecs() {
					// TODO Auto-generated method stub
					return 10;
				}
			})
            .withAutoScalerCallback(new Action1<AutoScaleAction>() {
				@Override
				public void call(AutoScaleAction arg0) {
					// TODO Auto-generated method stub
					logger.info("AutoScale ruleName={}, type={}",arg0.getRuleName(), arg0.getType());
					long currentSec =  getCurrentSec();
					if (arg0.getType() == AutoScaleAction.Type.Up )
					{
						ScaleUpAction upAction = (ScaleUpAction) arg0;
						int scaleUp = upAction.getScaleUpCount();
						long deltaDelay = currentSec - lastDeclineOfferInSec.get();
						long deltaInterval = currentSec - lastScaleInSec.get();
						logger.info("Scale up count={}, since last decline offer={},  delay={}, since last scale={}, scale interval={} ",scaleUp, deltaDelay, scaleWaitNewOfferSec, deltaInterval,scaleIntervalBySecs);
						if (deltaDelay >=  scaleWaitNewOfferSec && deltaInterval>=scaleIntervalBySecs) 
						{
							scaleClusterAgent.scaleUp(scaleUp);
							lastScaleInSec.getAndSet(getCurrentSec());
						}
					}
					else if (arg0.getType() == AutoScaleAction.Type.Down)
					{
						ScaleDownAction downAction = (ScaleDownAction) arg0;
						Collection<String> hosts = downAction.getHosts();
						long deltaInterval = currentSec - lastScaleInSec.get();
						logger.info("Scale down hosts={},since last scale={}, scale interval={}",hosts,deltaInterval,scaleIntervalBySecs);
						if (deltaInterval>=scaleIntervalBySecs) 
						{
							scaleClusterAgent.scaleDown(hosts.toArray(new String[hosts.size()]));
							lastScaleInSec.getAndSet(getCurrentSec());
						}
					}
				}
			});
    }
    this.fenzoScheduler = builder.build();

  }
  
  private Long getCurrentSec()
  {
	  return new Date().getTime()/1000;
  }

  @Override
  public void registered(SchedulerDriver schedulerDriver, Protos.FrameworkID frameworkID, Protos.MasterInfo masterInfo) {
    logger.info("registered() master={}:{}, framework={}", masterInfo.getIp(), masterInfo.getPort(), frameworkID);
    fenzoScheduler.expireAllLeases();
  }

  @Override
  public void reregistered(SchedulerDriver schedulerDriver, Protos.MasterInfo masterInfo) {
    logger.info("reregistered()");
    fenzoScheduler.expireAllLeases();
  }

  @Override
  public void resourceOffers(SchedulerDriver schedulerDriver, List<Protos.Offer> offers) {

    logger.info("resourceOffers() with {} offers", offers.size());
    for(Protos.Offer offer: offers) {
        logger.info("Adding offer " + offer.getId() + " from host " + offer.getHostname());
        leasesQueue.offer(new VMLeaseObject(offer));
    }
  }
  
  private int getDesiredInstances()
  {
	  return this.monitorAgent.getDesiredInstances();
  }
  /**
   * Run scheduling loop until shutdown is called.
   * This sample implementation shows the general pattern of using Fenzo's task scheduler. Scheduling occurs in a
   * continuous loop, iteratively calling {@link TaskScheduler#scheduleOnce(List, List)} method, passing in the
   * list of tasks currently pending launch, and a list of any new resource offers obtained from mesos since the last
   * time the lease queue was drained. The call returns an assignments result object from which any tasks assigned
   * can be launched via the mesos driver. The result contains a map with hostname as the key and assignment result
   * as the value. The assignment result contains the resource offers of the host that were used for the assignments
   * and a list of tasks assigned resources from the host. The resource offers were removed from the internal state in
   * Fenzo. However, the task assignments are not updated, yet. If the returned assignments are being used to launch
   * the tasks in mesos, call Fenzo's task assigner for each task launched to indicate that the task is being launched.
   * If the returned assignments are not being used, the resource offers in the assignment results must either be
   * rejected in mesos, or added back into Fenzo explicitly.
   * TODO: should loop be used or it can be triggered anytime an offer happens, which has interval already
   */
   void runAll() {
	   List<VirtualMachineLease> newLeases = new ArrayList<>();

	   while(true) {
		   
		   if(isShutdown.get()) return;

		   int desiredInst = this.getDesiredInstances();
		   logger.info("Run Scheduling Loop, Number of instances: pending={}, running={}, taskQueue={}, pendingTasks={}, desired={}",
				        pendingInstances.size(), runningInstances.size(), taskQueue.size(), pendingTasksMap.size(), desiredInst);
		   
		   int delta = desiredInst -( runningInstances.size() + pendingInstances.size() + taskQueue.size() + pendingTasksMap.size());
		   if (delta >0)
		   {
			   logger.info("Add to TaskQueue: " + delta);
			   for (int i = 0; i < delta; i++) {
				   taskQueue.offer(getTaskRequest());
			   }
		   }
		   if (delta <0)
		   {
			   delta = delta * -1;
			   Iterator<String> taskIdItr = launchedTaskHostMap.keySet().iterator();
			   String taskId;
			   for (int i = 0; i < delta && taskIdItr.hasNext(); i++) {
				   taskId =taskIdItr.next();
				   logger.info("remove task: {}/{} = {}", i, delta, taskId);
				   mesosSchedulerDriverRef.get().killTask(getTaskId(taskId));
			   }
			   
		   }

		   newLeases.clear();
	       List<TaskRequest> newTaskRequests = new ArrayList<>();

        TaskRequest taskRequest=null;
        try {
              taskRequest = pendingTasksMap.size()==0?
                      taskQueue.poll(5, TimeUnit.SECONDS) :
                      taskQueue.poll(1, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException ie) {
              logger.error("Error polling task queue: " + ie.getMessage());
        }
        if(taskRequest!=null) {
              taskQueue.drainTo(newTaskRequests);
              newTaskRequests.add(0, taskRequest);
              for(TaskRequest request: newTaskRequests)
                  pendingTasksMap.put(request.getId(), request);
        }
        leasesQueue.drainTo(newLeases);
        SchedulingResult schedulingResult = fenzoScheduler.scheduleOnce(new ArrayList<>(pendingTasksMap.values()), newLeases);
        logger.info("Fenzo scheduleOnce result=" + schedulingResult);
        Map<String,VMAssignmentResult> resultMap = schedulingResult.getResultMap();
        if(!resultMap.isEmpty()) {
              for(VMAssignmentResult result: resultMap.values()) {
                  List<VirtualMachineLease> leasesUsed = result.getLeasesUsed();
                  List<Protos.TaskInfo> taskInfos = new ArrayList<>();
                  StringBuilder stringBuilder = new StringBuilder("Launching on VM " + leasesUsed.get(0).hostname() + " tasks ");
                  final Protos.SlaveID slaveId = leasesUsed.get(0).getOffer().getSlaveId();
                  for(TaskAssignmentResult t: result.getTasksAssigned()) {
                      stringBuilder.append(t.getTaskId()).append(", ");
                      taskInfos.add(getTaskInfo(slaveId, t.getTaskId(), t.getRequest()));
                      pendingInstances.add(t.getTaskId());
                      // remove task from pending tasks map and put into launched tasks map
                      // (in real world, transition the task state)
                      pendingTasksMap.remove(t.getTaskId());
                      launchedTaskHostMap.put(t.getTaskId(), leasesUsed.get(0).hostname());
                      fenzoScheduler.getTaskAssigner().call(t.getRequest(), leasesUsed.get(0).hostname());
                  }
                  List<Protos.OfferID> offerIDs = new ArrayList<>();
                  for(VirtualMachineLease l: leasesUsed)
                      offerIDs.add(l.getOffer().getId());
                  logger.info(stringBuilder.toString());
                  mesosSchedulerDriverRef.get().launchTasks(offerIDs, taskInfos);
              }
        }
        // insert a short delay before scheduling any new tasks or tasks from before that haven't been launched yet.
        try{Thread.sleep(this.scheduleDelayTS);}catch(InterruptedException ie){}

	   }
  }

  private String nextTaskId() {
	  return taskName+"."+UUID.randomUUID();
  }
  
  // TODO should read from JSON
  private TaskRequest getTaskRequest() {
      final String taskId = nextTaskId();
      return new TaskRequest() {
          @Override
          public String getId() {
              return taskId;
          }

          @Override
          public String taskGroupName() {
              return frameworkName;
          }

          @Override
          public double getCPUs() {
              return taskCPU;
          }

          @Override
          public double getMemory() {
              return taskMEM;
          }

          @Override
          public double getNetworkMbps() {
              return taskNET;
          }

          @Override
          public double getDisk() {
              return taskDISK;
          }

          @Override
          public int getPorts() {
              return 1;
          }

          @Override
          public List<? extends ConstraintEvaluator> getHardConstraints() {
        	  List<ConstraintEvaluator> list = new ArrayList<ConstraintEvaluator>();
        	  list.add(new ExclusiveHostConstraint()); // Force a exclusive host as we are using HOST network
        	  if (!hostAttribute[0].equals(TASK_HOST_ATTRIBUTE_NAME_NOT_DEFINED))
        	  {
        	   list.add( new HostAttrValueConstraint(hostAttribute[0], new Func1<java.lang.String,java.lang.String>() {

  				@Override
  				public String call(String arg0) {
  					// TODO, as we only have one framework. Otherwise need a task id to framework map and framework to attribute map
  					return hostAttribute[1];
  				}
              	  
  			} ));
        	  }
        	  return list;
          }

          @Override
          public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
              return null;
          }
      };
  }
  
  private Protos.TaskID getTaskId(String id)
  {
	  return Protos.TaskID.newBuilder()
		        .setValue(id).build();
  }
  
  private Protos.TaskInfo getTaskInfo(Protos.SlaveID slaveID, final String taskIdValue, final TaskRequest request) {
    // generate a unique task ID
    Protos.TaskID taskId = getTaskId(taskIdValue);

    logger.info("Launching task {}", taskId.getValue());
    // docker image info
    Protos.ContainerInfo.DockerInfo.Builder dockerInfoBuilder = Protos.ContainerInfo.DockerInfo.newBuilder();
    dockerInfoBuilder.setImage(imageName);
    dockerInfoBuilder.setNetwork(Protos.ContainerInfo.DockerInfo.Network.HOST);

    // container info
    Protos.ContainerInfo.Builder containerInfoBuilder = Protos.ContainerInfo.newBuilder();
    containerInfoBuilder.setType(Protos.ContainerInfo.Type.DOCKER);
    containerInfoBuilder.setDocker(dockerInfoBuilder.build());

    Protos.Environment.Builder environmentBuilder = Protos.Environment.newBuilder();
    for (Map.Entry<String, String> entry  : envMap.entrySet())
    {
    	if (entry.getKey().startsWith(KEY_PREFIX)) continue;
    	environmentBuilder.addVariables(Protos.Environment.Variable.newBuilder()
    		      .setName(entry.getKey()).setValue(entry.getValue()));
    }
    // create task to run. TODO. Need more information
    Protos.TaskInfo task = Protos.TaskInfo.newBuilder()
        .setName(taskName)
        .setTaskId(taskId)
        .setSlaveId(slaveID)
        .addResources(Protos.Resource.newBuilder()
            .setName("cpus")
            .setType(Protos.Value.Type.SCALAR)
            .setScalar(Protos.Value.Scalar.newBuilder().setValue(request.getCPUs())))
        .addResources(Protos.Resource.newBuilder()
            .setName("mem")
            .setType(Protos.Value.Type.SCALAR)
            .setScalar(Protos.Value.Scalar.newBuilder().setValue(request.getMemory())))
        .setContainer(containerInfoBuilder)
        .setCommand(Protos.CommandInfo.newBuilder().setShell(false).setEnvironment(environmentBuilder))
        .build();

    return task;
  }

  @Override
  public void offerRescinded(SchedulerDriver schedulerDriver, Protos.OfferID offerID) {
    logger.info("offerRescinded()");
    fenzoScheduler.expireLease(offerID.getValue());
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus taskStatus) {

    final String taskId = taskStatus.getTaskId().getValue();

    logger.info("statusUpdate() task {} is in state {}",
        taskId, taskStatus.getState());

    switch (taskStatus.getState()) {
      case TASK_RUNNING:
        runningInstances.add(taskId);
        pendingInstances.remove(taskId);
        break;
      case TASK_FAILED:
      case TASK_LOST:
      case TASK_FINISHED:
        fenzoScheduler.getTaskUnAssigner().call(taskStatus.getTaskId().getValue(), launchedTaskHostMap.remove(taskId));
        pendingInstances.remove(taskId);
        runningInstances.remove(taskId);
        break;
    }

    logger.info("Post statusUpdate, Number of instances: pending={}, running={}, taskQueue={}, pendingTasks={}, desired={}",
        pendingInstances.size(), runningInstances.size(), taskQueue.size(), pendingTasksMap.size(), this.getDesiredInstances());
  }

  @Override
  public void frameworkMessage(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, byte[] bytes) {
    logger.info("frameworkMessage()");
  }

  @Override
  public void disconnected(SchedulerDriver schedulerDriver) {
    logger.info("disconnected()");
  }

  @Override
  public void slaveLost(SchedulerDriver schedulerDriver, Protos.SlaveID slaveID) {
    logger.info("slaveLost()");
  }

  @Override
  public void executorLost(SchedulerDriver schedulerDriver, Protos.ExecutorID executorID, Protos.SlaveID slaveID, int i) {
    logger.info("executorLost()");
  }

  @Override
  public void error(SchedulerDriver schedulerDriver, String s) {
    logger.error("error() {}", s);
  }

}
