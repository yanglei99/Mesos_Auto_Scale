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

package com.mesos.framework.impl;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mesos.framework.TaskMonitor;

public class MesosMonitor implements TaskMonitor {
	
	  private static final Logger logger = LoggerFactory.getLogger(MesosMonitor.class);

	  private static final String MESOS_KEY_PREFIX = KEY_PREFIX+"mesos.";
	  private static final String CPU_THRESHOLD_MAX_KEY = MESOS_KEY_PREFIX+"threshold.max.cpu";
	  private static final String MEM_THRESHOLD_MAX_KEY = MESOS_KEY_PREFIX+"threshold.max.mem";
	  private static final String CPU_THRESHOLD_MIN_KEY = MESOS_KEY_PREFIX+"threshold.min.cpu";
	  private static final String MEM_THRESHOLD_MIN_KEY = MESOS_KEY_PREFIX+"threshold.min.mem";

	  private static final float THRESHOLD_MAX_CPU_UNDEFINED = 0.8F;
	  private static final float THRESHOLD_MIN_CPU_UNDEFINED = 0.2F;
	  
	  private static final float THRESHOLD_MAX_MEM_UNDEFINED = 0.8F;
	  private static final float THRESHOLD_MIN_MEM_UNDEFINED = 0.01F;

	  private static final String THRESHOLD_MAX_MATCH_ALL_KEY = MESOS_KEY_PREFIX+"threshold.max.match.all";
	  private static final boolean THRESHOLD_MAX_MATCH_ALL_UNDEFINED = false;

	  private static final String THRESHOLD_MIN_MATCH_ALL_KEY = MESOS_KEY_PREFIX+"threshold.min.match.all";
	  private static final boolean THRESHOLD_MIN_MATCH_ALL_UNDEFINED = false;

	  
	  private static final String SCALE_MULTIPLIER_KEY = MESOS_KEY_PREFIX+"scale.multiplier";
	  private static final float SCALE_MULTIPLIER_UNDEFINED = 0.2F;

	  private static final String SCALEUP_MAX_MULTIPLIER_KEY = MESOS_KEY_PREFIX+"scale.up.max.multiplier";
	  private static final float SCALEUP_MAX_MULTIPLIER_UNDEFINED =2;

	  private static final String MONITOR_INTERVAL_KEY = MESOS_KEY_PREFIX+"monitor.interval";
	  private static final long MONITOR_INTERVAL_UNDEFINED = 5L;

	  private final Map<String, Float> thresholds;
	  private final AtomicReference<Long> lastOverThresholdInTS = new AtomicReference<Long>(0L);
	  private final AtomicReference<Map<String, Float>> lastOverThresholdReason = new AtomicReference<Map<String, Float>>();

	  private final boolean thresholdMaxMatchAll;
	  private final boolean thresholdMinMatchAll;

	  private final int minInstances;
	  private final int maxInstances;
	  private final float multiplier;

	  private final long timeInterval;
	  
	  private final AtomicReference<Integer> desiredInstances;
	  
	  private final Map<String, String> launchedTaskHostMap;
	  
	  private float lastTotalCPU = 0;
	  private long lastStatisticsTS =0L;
	  
	  public MesosMonitor(HashMap<String ,String> config, int minInstances, Map<String, String> launchedTaskHostMap)
	 {
		  this.thresholds = new HashMap<String, Float>();
		  thresholds.put(CPU_THRESHOLD_MAX_KEY, (config.containsKey(CPU_THRESHOLD_MAX_KEY)) ? (new Float(config.get(CPU_THRESHOLD_MAX_KEY))): THRESHOLD_MAX_CPU_UNDEFINED);
		  thresholds.put(MEM_THRESHOLD_MAX_KEY, (config.containsKey(MEM_THRESHOLD_MAX_KEY)) ? (new Float(config.get(MEM_THRESHOLD_MAX_KEY))): THRESHOLD_MAX_MEM_UNDEFINED);
		 
		  thresholds.put(CPU_THRESHOLD_MIN_KEY, (config.containsKey(CPU_THRESHOLD_MIN_KEY)) ? (new Float(config.get(CPU_THRESHOLD_MIN_KEY))): THRESHOLD_MIN_CPU_UNDEFINED);
		  thresholds.put(MEM_THRESHOLD_MIN_KEY, (config.containsKey(MEM_THRESHOLD_MIN_KEY)) ? (new Float(config.get(MEM_THRESHOLD_MIN_KEY))): THRESHOLD_MIN_MEM_UNDEFINED);

		  this.thresholdMaxMatchAll =  (config.containsKey(THRESHOLD_MAX_MATCH_ALL_KEY)) ? (new Boolean(config.get(THRESHOLD_MAX_MATCH_ALL_KEY))): THRESHOLD_MAX_MATCH_ALL_UNDEFINED;
		  this.thresholdMinMatchAll =  (config.containsKey(THRESHOLD_MIN_MATCH_ALL_KEY)) ? (new Boolean(config.get(THRESHOLD_MIN_MATCH_ALL_KEY))): THRESHOLD_MIN_MATCH_ALL_UNDEFINED;

		  this.minInstances=minInstances;
		  this.maxInstances =  (int) Math.ceil(minInstances * (config.containsKey(SCALEUP_MAX_MULTIPLIER_KEY) ? new Float(config.get(SCALEUP_MAX_MULTIPLIER_KEY)): SCALEUP_MAX_MULTIPLIER_UNDEFINED));

		  this.multiplier =  (config.containsKey(SCALE_MULTIPLIER_KEY)) ? (new Float(config.get(SCALE_MULTIPLIER_KEY))): SCALE_MULTIPLIER_UNDEFINED;

		  this.timeInterval =  ((config.containsKey(MONITOR_INTERVAL_KEY)) ? (new Long(config.get(MONITOR_INTERVAL_KEY))): MONITOR_INTERVAL_UNDEFINED)*1000;

		  this.desiredInstances = new AtomicReference<Integer>(minInstances);
		  
		  this.launchedTaskHostMap = launchedTaskHostMap;
		  
		  Runnable runnable = new Runnable() {
		  
			  public void run() {
			    while (true) {
			    	
			      calculateScale();
			      try {
			       Thread.sleep(timeInterval);
			      } catch (InterruptedException e) {
			        e.printStackTrace();
			      }
			      }
			    }
		  };
		  
		  Thread thread = new Thread(runnable);
		  thread.start();

	 }
	  
	 private int calculateScale()
	 {
		 Set<String> taskIds = launchedTaskHostMap.keySet();
		 Set<String> hosts = new HashSet<String>(launchedTaskHostMap.values());
		 
		 float totalCPU = 0;
		 float totalMem = 0;
		 int taskCount = 0 ; 
		 float cpus_limit = 0; 

		 for ( String host: hosts)
		 {
			 JsonArray rootArray = getStatistics(host) ;
			 if (rootArray !=null)
			 {
				 for (int i =0; i < rootArray.size(); i++)
				 {
					 JsonObject element = rootArray.get(i).getAsJsonObject();
					 
					 if (taskIds.contains(element.get("executor_id").getAsString()))
					 {
						 JsonObject statistics = element.get("statistics").getAsJsonObject();
						 logger.info("host={}, statistics={}",host, statistics);
						 if (cpus_limit == 0) cpus_limit = statistics.get("cpus_limit").getAsFloat();// assume all tasks having the same limit
						 //https://github.com/mesosphere/marathon-autoscale/blob/master/marathon-autoscale.py
						 float cpus_time = (statistics.get("cpus_system_time_secs").getAsFloat()+statistics.get("cpus_user_time_secs").getAsFloat());
						 float mem_utilization = statistics.get("mem_rss_bytes").getAsFloat()/statistics.get("mem_limit_bytes").getAsFloat();
						 totalCPU+=cpus_time;
						 totalMem+=mem_utilization;
						 taskCount++;
					 }
				 }
			 }
		 }
		 if (taskCount==0) return 0;
		 
		 // re-calculate the CPU usage as what is received is total
		 long currentTS = new Date().getTime(); // Not as accurate as TS from statistics but simple
		 
		 if (this.lastStatisticsTS==0) // assume the first statistics does not need to trigger scale
		 {
			 this.lastStatisticsTS= currentTS;
			 this.lastTotalCPU = totalCPU;
			 
			 totalCPU = 0;
		 }else{
			 float deltaCPU=(totalCPU-this.lastTotalCPU) *1000 /(currentTS-this.lastStatisticsTS)/cpus_limit  ;
			 this.lastStatisticsTS= currentTS;
			 this.lastTotalCPU = totalCPU;
			 totalCPU=deltaCPU;
		 }
		
		 totalCPU = totalCPU/taskCount;
		 totalMem = totalMem/taskCount;
		 logger.info("taskCount={}, cpuAvg={}, memAvg={}",taskCount, totalCPU,totalMem);
		 
		 Map<String, Float> scaleUpUsage = new HashMap<String, Float>();
		 if (totalCPU > thresholds.get(CPU_THRESHOLD_MAX_KEY))
			 scaleUpUsage.put(CPU_THRESHOLD_MAX_KEY, totalCPU);
		 if (totalMem > thresholds.get(MEM_THRESHOLD_MAX_KEY))
			 scaleUpUsage.put(MEM_THRESHOLD_MAX_KEY, totalMem);
		 
		 // Scale down need to check negative situation due to instance just scaled down the number will not be accurate
		 Map<String, Float> scaleDownUsage = new HashMap<String, Float>();
		 if (totalCPU > 0 && totalCPU < thresholds.get(CPU_THRESHOLD_MIN_KEY))
			 scaleDownUsage.put(CPU_THRESHOLD_MIN_KEY, totalCPU);
		 if (totalMem > 0 && totalMem < thresholds.get(MEM_THRESHOLD_MIN_KEY))
			 scaleDownUsage.put(MEM_THRESHOLD_MIN_KEY, totalMem);
		 
		 
		 // Use taskCount to determine scale, than desiredInstances in case there is a difference
 		 if (satisfyScaleMatch(this.thresholdMaxMatchAll,scaleUpUsage.size())) // Scale up take precedents 
		 {
			 this.lastOverThresholdInTS.set(currentTS);
			 this.lastOverThresholdReason.set(scaleUpUsage);
   			 int scaleup = taskCount + (int) Math.ceil(multiplier * taskCount);
   			 if (scaleup > maxInstances) scaleup= maxInstances;
   			 if (desiredInstances.getAndSet(scaleup) < scaleup)
   				logger.info("Scale up to new desired instances={},reason={}, matchAll={}",scaleup,scaleUpUsage,this.thresholdMaxMatchAll);

			 return 1;
		 }
		 if (satisfyScaleMatch(this.thresholdMinMatchAll,scaleDownUsage.size() )) 
		 {
			 this.lastOverThresholdInTS.set(currentTS);
			 this.lastOverThresholdReason.set(scaleDownUsage);
			 int scaleDown = taskCount - (int) Math.ceil(multiplier * taskCount);
			 if (scaleDown < minInstances) scaleDown=minInstances;
			 if (desiredInstances.getAndSet(scaleDown) > scaleDown)
	   			logger.info("Scale down to new desired instances={},reason={}, matchAll={}",scaleDown,scaleDownUsage,this.thresholdMinMatchAll);
			 return -1;
		 }
		 return 0;

	 }
	 

	 
	 private boolean satisfyScaleMatch(boolean matchAll, int number)
	 {
		 return matchAll? number==2: number >0;
	 }
	 
	 /**
	  * a sample return
	  * [{"executor_id":"task-acmeair.9bd310f1-b009-4b26-8074-864d3420988a","executor_name":"Command Executor (Task: task-acmeair.9bd310f1-b009-4b26-8074-864d3420988a) (Command: NO EXECUTABLE)","framework_id":"7a233ad6-e506-42dd-a9ff-a9821d93d3e3-0089","source":"task-acmeair.9bd310f1-b009-4b26-8074-864d3420988a","statistics":{"cpus_limit":1.1,"cpus_system_time_secs":0.1,"cpus_user_time_secs":0.45,"mem_limit_bytes":1107296256,"mem_rss_bytes":26841088,"timestamp":1458334777.05838}}]
	  * @param host
	  * @return
	  */
	 private JsonArray getStatistics(String host)
	 {
		 try{
			 String sURL = "http://"+host+":5051/monitor/statistics.json"; 
	
			 URL url = new URL(sURL);
			 HttpURLConnection request = (HttpURLConnection) url.openConnection();
			 request.connect();
	
			 JsonParser jp = new JsonParser(); 
			 JsonElement root = jp.parse(new InputStreamReader((InputStream) request.getContent())); 
			 return root.getAsJsonArray(); 
		 }catch (Exception e)
		 {
			logger.error(e.getMessage());
		 }
		 return null;
	 }
	 
	@Override
	public int getDesiredInstances() {
		return this.desiredInstances.get();
	}

	@Override
	public long getLastOverThresholdTS() {
		return this.lastOverThresholdInTS.get();
	}

	@Override
	public Map<String, Float> getLastOverThresholdReason() {
		return this.lastOverThresholdReason.get();
	}

	@Override
	public Map<String, Float> getThreshold() {
		return thresholds;
	}
	  
}