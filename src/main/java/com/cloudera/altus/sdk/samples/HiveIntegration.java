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
import com.cloudera.altus.dataeng.model.HiveJobRequest;
import com.cloudera.altus.dataeng.model.JobRequest;
import com.cloudera.altus.dataeng.model.JobStatus;
import com.cloudera.altus.dataeng.model.SubmitJobsRequest;
import com.cloudera.altus.dataeng.model.SubmitJobsResponse;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIntegration extends BaseIntegration {

	private static final Logger LOG = LoggerFactory.getLogger(HiveIntegration.class);

	public static void main(String[] args) {
		String clusterName = "sample-Hive";

		HiveIntegration builder = new HiveIntegration();
		DataengClient client = builder.createClient();

		try {
      ClusterStatus clusterCreationStatus = builder.createAWSCluster(client, clusterName, "HIVE_ON_SPARK");


			/*
			Valid values for Cluster status can be found at in the Altus documentation:
			https://www.cloudera.com/documentation/altus/topics/altaws_declu_clusters.html#cluster_status
			*/
			if (ClusterStatus.CREATED.equals(clusterCreationStatus)) {
				/*
				In this scenario the cluster is already created when submitting the Job. However, you can also
				submit the job without waiting for cluster creation. Job will be in queue until cluster is created.
				*/
				builder.createJob(client, clusterName);
				LOG.info("Successfully created cluster and submitted Hive job to it");
			} else {
				LOG.error("An error has occurred. Cluster was not created as expected");
			}
		} catch (AltusServiceException ase) {
			LOG.error("Error occurred while trying to create and submit a Hive Job " + ase.getMessage());
		}
	}

	@Override
	void createJob(DataengClient client, String clusterName) {
		SubmitJobsRequest submitJobsRequest = new SubmitJobsRequest();
		submitJobsRequest.setClusterName(clusterName);
		ArrayList<JobRequest> jobs = new ArrayList<>();
		JobRequest job = new JobRequest();
		HiveJobRequest hiveJob = new HiveJobRequest();
		/* The Hive script to execute. It is used to create tables. */
		hiveJob.setScript("s3a://cloudera-altus-data-engineering-samples/hive/program/med-part1.hql");
		/* Parameters for the Hive script. */
		List<String> params = new ArrayList<String>();
		params.add("HOSPITALS_PATH=s3a://cloudera-altus-data-engineering-samples/hive/data/hospitals/");
		params.add("READMISSIONS_PATH=s3a://cloudera-altus-data-engineering-samples/hive/data/readmissionsDeath/");
		params.add("EFFECTIVECARE_PATH=s3a://cloudera-altus-data-engineering-samples/hive/data/effectiveCare/");
		params.add("GDP_PATH=s3a://cloudera-altus-data-engineering-samples/hive/data/GDP/");

		hiveJob.setParams(params);
		job.setName("sample-hive-job");
		job.setHiveJob(hiveJob);
		jobs.add(job);
		submitJobsRequest.setJobs(jobs);
		SubmitJobsResponse response = client.submitJobs(submitJobsRequest);
    if (JobStatus.FAILED.equals(response)
				|| JobStatus.TERMINATING.equals(response)) {
      LOG.error("Unable to create Job request ");
    }
	}
}
