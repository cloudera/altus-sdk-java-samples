/*
 *
 *  *
 *  *  * Copyright (c) 2017 Cloudera, Inc. All Rights Reserved.
 *  *  *
 *  *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  *  * you may not use this file except in compliance with the License.
 *  *  * You may obtain a copy of the License at
 *  *  *
 *  *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 */
package com.cloudera.altus.sdk.samples;

import com.cloudera.altus.AltusServiceException;
import com.cloudera.altus.dataeng.api.DataengClient;
import com.cloudera.altus.dataeng.model.ClusterStatus;
import com.cloudera.altus.dataeng.model.CreateAWSClusterRequest;
import com.cloudera.altus.dataeng.model.CreateAWSClusterRequest.AutomaticTerminationConditionEnum;
import com.cloudera.altus.dataeng.model.CreateAWSClusterResponse;
import com.cloudera.altus.dataeng.model.JobRequest;
import com.cloudera.altus.dataeng.model.SparkJobRequest;

import java.io.IOException;
import java.util.ArrayList;

import org.ini4j.Wini;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  It will create a cluster with minimal inputs and default values.
 *  After creating the cluster, it will submit the job.
 *  The cluster gets deleted after submission of the job.
 */
public class SparkAllInOneIntegration extends BaseIntegration {

	private static final Logger LOG = LoggerFactory.getLogger(SparkAllInOneIntegration.class);

	public static void main(String[] args) {
		SparkAllInOneIntegration builder = new SparkAllInOneIntegration();
		String clusterName = "Sample-Spark-AllInOne-2";

		try {
			DataengClient client = builder.createClient();
			builder.createJob(client, clusterName);
      LOG.info("Successfully created cluster and Spark job all in one step");
		} catch (AltusServiceException ase) {
			LOG.error("Error occurred with trying to create and submit Spark job " + ase.getMessage());
		}
	}

	@Override
	void createJob(DataengClient client, String clusterName) {
		/* Create cluster with minimal input and use defaults (e.g. for ebs config). */
		CreateAWSClusterRequest request = new CreateAWSClusterRequest();
		request.setClusterName(clusterName);

		try {
			Wini ini = getIniFile();
			request.setCdhVersion(ini.get("AWSCluster", "cdhVersion"));

      String privateKeyFileLocation = ini.get("credentials", "ssh_private_key_location");
      String sshPrivatekey = getSshPrivateKeyContents(privateKeyFileLocation);

			request.setSshPrivateKey(sshPrivatekey);
			request.setServiceType("SPARK");
			request.setInstanceType(ini.get("AWSCluster", "instanceType"));
			request.setWorkersGroupSize(ini.get("AWSCluster", "workerSize", int.class));
			request.setEnvironmentName(ini.get("AWSCluster", "environmentName"));
			request.setClouderaManagerUsername(ini.get("AWSCluster", "CMUsername"));
			request.setClouderaManagerPassword(ini.get("AWSCluster", "CMPassword"));

			/* Add Job specific information. */
			ArrayList<JobRequest> jobs = new ArrayList<>();
			JobRequest job = new JobRequest();
			SparkJobRequest sparkJob = new SparkJobRequest();
			ArrayList<String> jars = new ArrayList<String>();
			jars.add("s3a://cloudera-altus-data-engineering-samples/spark/medicare/program/altus-sample-medicare-spark2x.jar");
			sparkJob.setJars(jars);
			sparkJob.setMainClass("com.cloudera.altus.sample.medicare.transform");
			ArrayList<String> args = new ArrayList<String>();
			/* Add parameters for the jar file. */
			args.add("s3a://cloudera-altus-data-engineering-samples/spark/medicare/input/");

			args.add(ini.get("jobs", "outputLocation"));
			sparkJob.setApplicationArguments(args);
			job.setName("sample-SparkAllInOne-Job");
			job.setSparkJob(sparkJob);
			jobs.add(job);

			/*
			Cluster will  terminate after successful completion of the job since EMPTY_JOB_QUEUE
			https://www.cloudera.com/documentation/altus/Shared/altus_dejob_jobs.html#unique_1104585979
			*/
			request.setJobs(jobs);
			request.setAutomaticTerminationCondition(AutomaticTerminationConditionEnum.EMPTY_JOB_QUEUE);
			CreateAWSClusterResponse response = client.createAWSCluster(request);

			if (ClusterStatus.FAILED.equals(response.getCluster().getStatus())
          || ClusterStatus.TERMINATING.equals(response.getCluster().getStatus())) {
			  LOG.error("Unable to create AWS cluster ");
      }
      LOG.info("Successfully creating cluster: " + response.getCluster().getClusterName()) ;
		} catch (IOException ioe) {
			LOG.error("Unable to load SampleResources.ini file " + ioe.getMessage());
			throw new RuntimeException("Unable to load SampleResources.ini file "
																		 + ioe.getMessage());
		} catch (AltusServiceException ase) {
			LOG.error(
					"Altus exception occurred when trying to create and submit job to a "
							+ "Spark cluster - " +  " http code " + ase.getHttpCode() + " : "
							+ ase.getMessage());
			throw ase;
		}
	}
}
