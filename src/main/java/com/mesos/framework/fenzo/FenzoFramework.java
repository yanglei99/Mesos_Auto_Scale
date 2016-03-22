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
 * reference https://github.com/codefutures/mesos-docker-tutorial/blob/master/src/main/java/com/codefutures/tutorial/mesos/docker/ExampleFramework.java
 * With enhancement:
 * 1. Enable mesos-dns
 * 2. Enable passing in environment variable
 * 3. Use Fenzo Scheduler
 */
package com.mesos.framework.fenzo;

import com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;

/**
 * Example framework that will run a scheduler that in turn will cause
 * Docker containers to be launched.
 *
 * Source code adapted from the example that ships with Mesos.
 */
public class FenzoFramework {

  /** Show command-line usage. */
  private static void usage() {
    String name = FenzoFramework.class.getName();
    System.err.println("Usage: " + name + " master-ip-and-port framework-name docker-image-name number-of-instances environements");
  }

  /**
   * Command-line entry point.
   * <br/>
   * Example usage: java ExampleFramework 127.0.0.1:5050 acemair yanglei99/acmeair-nodejs 2
   */
  public static void main(String[] args) throws Exception {

    // check command-line args
    if (args.length < 4) {
      usage();
      System.exit(1);
    }
    
    // parse command-line args
    final String frameworkName = args[1];
    final String imageName = args[2];
    final int totalTasks = Integer.parseInt(args[3]);


    HashMap<String, String> envMap = new HashMap<String, String>();
    for (int i=4;i< args.length; i++)
    {
    	String[] envs= args[i].split("=");
    	envMap.put(envs[0], envs[1]);
    }

    // If the framework stops running, mesos will terminate all of the tasks that
    // were initiated by the framework but only once the fail-over timeout period
    // has expired. Using a timeout of zero here means that the tasks will
    // terminate immediately when the framework is terminated. For production
    // deployments this probably isn't the desired behavior, so a timeout can be
    // specified here, allowing another instance of the framework to take over.
    final int frameworkFailoverTimeout = 0;

    FrameworkInfo.Builder frameworkBuilder = FrameworkInfo.newBuilder()
        .setName(frameworkName)
        .setUser("") // Have Mesos fill in the current user.
        .setFailoverTimeout(frameworkFailoverTimeout); // timeout in seconds

    if (System.getenv("MESOS_CHECKPOINT") != null) {
      System.out.println("Enabling checkpoint for the framework");
      frameworkBuilder.setCheckpoint(true);
    }
    
    AtomicReference<MesosSchedulerDriver> ref = new AtomicReference<>();
    AtomicBoolean isShutdown = new AtomicBoolean(false);


    // create the scheduler
    final FenzoScheduler scheduler = new FenzoScheduler(
    	isShutdown,
    	ref,
    	frameworkName,
        imageName,
        totalTasks,
        envMap
    );

    // create the driver
    MesosSchedulerDriver driver;
    if (System.getenv("MESOS_AUTHENTICATE") != null) {
      System.out.println("Enabling authentication for the framework");

      if (System.getenv("DEFAULT_PRINCIPAL") == null) {
        System.err.println("Expecting authentication principal in the environment");
        System.exit(1);
      }

      if (System.getenv("DEFAULT_SECRET") == null) {
        System.err.println("Expecting authentication secret in the environment");
        System.exit(1);
      }

      Credential credential = Credential.newBuilder()
          .setPrincipal(System.getenv("DEFAULT_PRINCIPAL"))
          .setSecretBytes(ByteString.copyFrom(System.getenv("DEFAULT_SECRET").getBytes()))
          .build();

      frameworkBuilder.setPrincipal(System.getenv("DEFAULT_PRINCIPAL"));

      driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0], credential);
    } else {
      frameworkBuilder.setPrincipal("test-framework-java");

      driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0]);
    }
    ref.set(driver);
    new Thread() {
        public void run() {
            driver.run();
        }
    }.start();
    new Thread() {
        public void run() {
        	scheduler.runAll();
        }
    }.start();
    
    int status = driver.run() == Status.DRIVER_STOPPED ? 0 : 1;
    isShutdown.set(true);
    // Ensure that the driver process terminates.
    driver.stop();
    System.exit(status);
  }
}
