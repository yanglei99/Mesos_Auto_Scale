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

package com.mesos.framework.softlayer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mesos.framework.AutoScaleCluster;
import com.softlayer.api.*;
import com.softlayer.api.service.scale.Group;
import com.softlayer.api.service.scale.Member;

public class SoftlayerAgent implements AutoScaleCluster {
	
	private static final Logger logger = LoggerFactory.getLogger(SoftlayerAgent.class);

	private static final String SL_KEY_RREFIX = KEY_PREFIX+"softlayer.";
	private static final String USER_KEY = SL_KEY_RREFIX+"user";
	private static final String API_KEY = SL_KEY_RREFIX+"api";
	private static final String SCALE_GROUP_KEY = SL_KEY_RREFIX+"scale.group";

	private final ApiClient client;
	private final Group.Service scaleGroupService;
	private final AtomicReference<Boolean>  inScale = new AtomicReference<Boolean>(false) ;
	
	public SoftlayerAgent(Map<String ,String> config)
	{
		String user= config.get(USER_KEY);
		String api = config.get(API_KEY);
		
		if (user!=null && api!=null)
		{
			client = new RestApiClient().withCredentials(user, api);
		}else
		{
			logger.info("Donot have softlayer credential");
			client = null;
		}
		
		Long id = new Long(config.get(SCALE_GROUP_KEY));
		if (client!=null)
		{
			scaleGroupService = Group.service(client, id);
		}else
		{
			logger.info("Donot have the correct Auto Scale Group id: {}", id);
			scaleGroupService = null;
		}
	}

	@Override
	public void scaleUp(int delta) {
		
		logger.info("Scale: client={}, scaleGroupService={}, delta={}", client,scaleGroupService,delta);

		executeScale(delta);
	}

	private void executeScale (int delta)
	{

		if (client == null || scaleGroupService ==null ){
			logger.info("Not enough information to perform scale");
			return ;
		}
		
		if (inScale.getAndSet(true)) 
		{
			logger.info("Scale is already in progress");
			return ;
		}
		
		new Thread() {
		        public void run() {
		    		Group currentGroup = scaleGroupService.getObject();
		    		String statusName = currentGroup.getStatus().getName();
		    		Long max= currentGroup.getMaximumMemberCount();
		    		Long min= currentGroup.getMinimumMemberCount();
		    		int current = currentGroup.getVirtualGuestMembers().size();
		    		logger.info("Auto Scale Group: status={}, min={}, max={}, current={}, delta={}", statusName, min, max, current,delta);

		    		if (statusName.equalsIgnoreCase("active")){
		    			
		    			long newTotal = current + delta;
		    			if (newTotal < min) newTotal = min;
		    			if (newTotal > max) newTotal = max;
		    			
		    			if (newTotal != current)
		    			{
			    			logger.info("ScaleTo asynch: {}", newTotal);
			    			scaleGroupService.asAsync().scaleTo(newTotal, new ResponseHandler<List<Member>>() {
									
									@Override
									public void onSuccess(List<Member> arg0) {
							    		inScale.set(false);
					    				logger.info("Scale up asynch onSuccess: {}", arg0.size());
									}
									
									@Override
									public void onError(Exception arg0) {
							    		inScale.set(false);
					    				logger.info("Scale up asynch onError: {}", arg0.getMessage());
									}
								});
		    			}else
		    			{
				    		inScale.set(false);
		    				logger.info("Ignore scale as boundary reached");
		    			}
		    		}else {
			    		inScale.set(false);
	    				logger.info("Ignore scale as status is not active");
		    		}
		        }
		    }.start();
		    
	}
	@Override
	public void scaleDown(String[] hosts) {
		
		int delta = hosts.length;
		logger.info("Scale: client={}, scaleGroupService={}, delta={}", client,scaleGroupService,delta);

		executeScale(delta * -1);
	}


}
