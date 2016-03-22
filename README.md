# Mesos_Auto_Scale

This is a simple generic Mesos Framework that integrates with [Fenzo Scheduler](https://github.com/Netflix/Fenzo/wiki/Getting-Started) to demonstrate elasticity on the cloud. It leverages Softlayer Auto Scale Group for platform elasticity; it interacts with mesos monitor REST API to determine process elasticity.

![Image of Elastic with Fenzo on Mesos](images/fenzo_mesos_elastic.jpg)

There are only a few Mesos features enabled for the Framework for simplicity: 
* Define MESOS location
* Define the framework name
* Define Docker Image as task Executor
* Define initial instance number
* Define environment variable for task executors  
* Mesos-DNS is enabled: "task-frameworkname.frameworkname.mesos"
* NETWORK=HOST

There are a set of Fenzo features explored:

* Define framework configurations such as per task cpu, memory, disk, network, or resource allocation limit (max)
* Define offer expiration, delay schedule...
* Unique Host constraint is defined as we use HOST network
* Host Attribute Value Constraint is enabled when task.host.attribute is defined
* Auto Scale cluster is enabled when task.host.attribute is defined. The default auto scale implementation depends on Softlayer Auto Scale Group

![Image of Fenzo Docker Framework on Mesos](images/fenzo_mesos.jpg)

The Framework can be started on Mesos using Marathon. [A set of sample Marathon JSON are defined](marathon) to use [Acme Air and its workload](https://github.com/yanglei99/acmeair-nodejs) to verify the elasticity.

![Image of Fenzo Docker Framework on Mesos](images/fenzo_acmeair.jpg)


## Build the framework:

    sbt assembly

## Run the framework on Mesos

* w/o host attribute, nor auto scale

	curl -i -H 'Content-Type: application/json' -d@marathon/marathon-fenzo-acmeair.json $marathonIp:8080/v2/apps
	
* enabled host attribute constraint and auto scale by attribute
	
	curl -i -H 'Content-Type: application/json' -d@marathon/marathon-fenzo-acmeair-attribute.json $marathonIp:8080/v2/apps
	
	
### Run the framework locally

    sbt "run-main com.mesos.framework.fenzo.FenzoFramework $MESOS_HOST:5050 your-framework-name your-docker-image your-number-of-instances your-environment-for-container-and-task
	
	java -classpath target/scala-2.10/fenzo-framework.jar com.mesos.framework.fenzo.FenzoFramework $MESOS_HOST:5050 your-framework-name your-docker-image your-number-of-instances your-environment-for-container-and-task
	

###Configuration

### Executor Task Configuration for Fenzo Framework

Pass at the end of the application parameters in key=value form

Name | Default | Meaning
--- |:---:| ---
task.fenzo.cpu|1| cpu core for each task
task.fenzo.mem|1024| memory (MB) for each task
task.fenzo.disk|10| disk (MB) for each task
task.fenzo.net|0| network (MBPs) for each task
task.fenzo.max.cpu|| max CPU for all the tasks
task.fenzo.max.mem|| max memory (MB) for all the tasks
task.fenzo.max.disk|| max disk (MB) for all the task
task.fenzo.max.net|| max network (MBPs) for all the tasks
task.fenzo.min.idle|1| the min idle hosts for the framework
task.fenzo.max.idle|2| the max idle hosts for the framework
task.fenzo.lease.expire|10| the offer lease expiration in second
task.fenzo.scale.wait.offer|5| wait seconds since last lease expire before scale up.
task.fenzo.scale.up.delay|6| the auto scale up delay in second
task.fenzo.scale.down.delay|10| the auto scale down delay in second 
task.fenzo.scale.interval|60| the auto scale action interval
task.fenzo.schedule.delay|1| delay for scheduleOnce loop in second
task.fenzo.host.attribute|| the host attribute used to determine placement constraint and when cluster nodes needs to scale

### Auto Scale Softlayer Cluster related Configuration

Name | Default | Meaning
--- |:---:| ---
task.softlayer.user|| the softlayer userid
task.softlayer.api|| the softlayer api key
task.softlayer.scale.group|| the softlayer auto scale group id
	

Depends on [Softlayer Auto Scale Group](http://knowledgelayer.softlayer.com/learning/introduction-softlayer-auto-scale)

* Create Auto Scale Group with a slave image template. [Revise the script as post-install script to auto calculate slave IP and hostname](autoscale/start-mesos-slave.sh)

* To verify sale the Auto Scale group manually

	curl https://$username:$apikey@api.softlayer.com/rest/v3/SoftLayer_Scale_Group/$groupId/scaleTo/1


### Auto Scale Mesos Task related Configuration

Name | Default | Meaning
--- |:---:| ---
task.mesos.threshold.max.cpu|0.8| CPU % to trigger scale up
task.mesos.threshold.max.mem|0.8| memory % to trigger scale up
task.mesos.threshold.min.cpu|0.2| CPU % to trigger scale down
task.mesos.threshold.min.mem|0.01| memory % to trigger scale down
task.mesos.threshold.max.match.all|false| when set to true only trigger scale up when both CPU and MEM are over the limit (AND); when set to false, only need to match one condition (OR)
task.mesos.threshold.min.match.all|false| when set to true only trigger scale down when both CPU and MEM are over the limit (AND); when set to false, only need to match one condition (OR)
task.mesos.scale.multiplier|0.2| the multiplier to scale up and down
task.mesos.scale.up.max.multiplier|2| the max scale up multiplier. max_instances== multiplier*your-number-of-instances. min_instances==your-number-of-instances.
task.mesos.scale.match.all|false| when set to true only trigger scale up/down when both CPU and MEM are over the limit (AND); when set to false, only need to match one condition (OR)
task.mesos.monitor.interval|5|check task resource usage every n seconds

## Known Limitations

* Auto Scale up cluster nodes may be too aggressive. 
