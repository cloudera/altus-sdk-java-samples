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
import com.cloudera.altus.dataeng.model.JobRequest;
import com.cloudera.altus.dataeng.model.MR2JobRequest;
import com.cloudera.altus.dataeng.model.SubmitJobsRequest;
import com.cloudera.altus.dataeng.model.SubmitJobsResponse;

import java.io.IOException;
import java.util.ArrayList;

import org.ini4j.Ini;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MapreduceIntegration extends BaseIntegration {

	private static final Logger LOG = LoggerFactory.getLogger(MapreduceIntegration.class);

	public static void main(String[] args) {
		String clusterName = "sample-MR";
		MapreduceIntegration builder = new MapreduceIntegration();

		try {
			DataengClient client = builder.createClient();
			ClusterStatus clusterCreationStatus = builder.createAWSCluster(client, clusterName, "MR2");

			if (ClusterStatus.CREATED.equals(clusterCreationStatus)) {
				builder.createJob(client, clusterName);
			} else {
				LOG.error("Error creating cluster");
			}
		} catch (AltusServiceException ase) {
			LOG.error(
			    "Error occurred while trying to create and submit Mapreduce job " + ase.getMessage());
		}
	}

	@Override
	void createJob(DataengClient client, String clusterName) {
		SubmitJobsRequest submitJobsRequest = new SubmitJobsRequest();
		submitJobsRequest.setClusterName(clusterName);
		ArrayList<JobRequest> jobs = new ArrayList<>();
		JobRequest job = new JobRequest();
		MR2JobRequest mr2Job = new MR2JobRequest();
		ArrayList<String> jars = new ArrayList<String>();
		/* Path of jar file. */
		jars.add("s3a://cloudera-altus-data-engineering-samples/mr2/wordcount/program/altus-sample-mr2.jar");
		mr2Job.setJars(jars);

		/* Specify the main class for the jar. */
		mr2Job.setMainClass("com.cloudera.altus.sample.mr2.wordcount.WordCount");
		ArrayList<String> args = new ArrayList<String>();
		args.add("s3a://cloudera-altus-data-engineering-samples/mr2/wordcount/input/poetry/");

		try {
			Ini.Section ini = this.getIniFile().get("jobs");
			args.add(ini.get("outputLocation"));
			mr2Job.setArguments(args);
			/* This is the name that will appear in the Altus Job console. */
			job.setName("sample-Mapreduce-job");
			job.setMr2Job(mr2Job);
			jobs.add(job);
			submitJobsRequest.setJobs(jobs);
			SubmitJobsResponse response = client.submitJobs(submitJobsRequest);
			pollJobStatus(client, response.getJobs().get(0).getJobId());
		} catch (IOException ioe) {
			LOG.error("Unable to read SampleResources.ini file " + ioe.getMessage());
			throw new RuntimeException(
					"Unable to load SampleResources.ini file "+ ioe.getMessage());
		}
	}
}
