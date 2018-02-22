/*
 * Copyright (c) 2018 Cloudera, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.altus.sdk.samples;

import com.cloudera.altus.AltusServiceException;
import com.cloudera.altus.dataeng.api.DataengClient;
import com.cloudera.altus.dataeng.model.JobRequest;
import com.cloudera.altus.dataeng.model.SparkJobRequest;
import com.cloudera.altus.dataeng.model.SubmitJobsRequest;
import com.cloudera.altus.dataeng.model.SubmitJobsResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.ini4j.Wini;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class demonstrates how to create an Altus cluster on AWS that has
 * Spark installed on it, submit a Spark job and then terminate the cluster
 */
public class SparkIntegration extends BaseIntegration {

	private static final Logger LOG = LoggerFactory.getLogger(SparkIntegration.class);

	public static void main(String[] args) {
		SparkIntegration builder = new SparkIntegration();

		try {
			DataengClient client = builder.createClient();
			String clusterName = "Sample-Spark2";

			String clusterCreationStatus = builder.createAWSCluster(client, clusterName, "SPARK");
			if ("CREATED".equals(clusterCreationStatus)) {
				builder.createJob(client, clusterName);
				LOG.info("Successfully created cluster and submitted Spark job to it");
			} else {
				LOG.error("Cluster was not created as expected ");
			}
		} catch (AltusServiceException ase) {
			LOG.error("Error occurred when trying to create and submit Spark job " + ase.getMessage());
		}
	}

	@Override
	void createJob(DataengClient client, String clusterName) {
		try {
			Wini ini = getIniFile();

			SubmitJobsRequest submitSparkJobsRequest = new SubmitJobsRequest();
			submitSparkJobsRequest.setClusterName(clusterName);
			ArrayList<JobRequest> jobs = new ArrayList<>();
			JobRequest job = new JobRequest();

			//Spark specific information
			SparkJobRequest sparkJob = new SparkJobRequest();
			List<String> jars = new ArrayList<String>();
			jars.add("s3a://cloudera-altus-data-engineering-samples/spark/medicare/program/altus-sample-medicare-spark2x.jar");
			sparkJob.setJars(jars);
			sparkJob.setMainClass("com.cloudera.altus.sample.medicare.transform");
			ArrayList<String> args = new ArrayList<String>();
			args.add("s3a://cloudera-altus-data-engineering-samples/spark/medicare/input/");

			args.add(ini.get("jobs", "outputLocation"));
			sparkJob.setApplicationArguments(args);
			//This job name will appear in the Altus console
			job.setName("sample-Spark-Job");
			job.setSparkJob(sparkJob);
			jobs.add(job);
			submitSparkJobsRequest.setJobs(jobs);
			SubmitJobsResponse response = client.submitJobs(submitSparkJobsRequest);
			pollJobStatus(client, response.getJobs().get(0).getJobId());
		} catch (IOException ioe) {
			LOG.error("Unable to load SampleResources.ini file " + ioe.getMessage());
			throw new RuntimeException("Unable to load SampleResources.ini file "
																		 + ioe.getMessage());
		} catch (AltusServiceException ase) {
			LOG.error(
			    "Altus exception occurred when trying to submit job to a "
              + "Spark cluster - " +  " http code " + ase.getHttpCode()
							+ " : "  + ase.getMessage());
			throw ase;
		}
	}
}
