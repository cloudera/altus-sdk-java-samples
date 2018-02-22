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

import com.cloudera.altus.AltusClientException;
import com.cloudera.altus.AltusServiceException;
import com.cloudera.altus.client.AltusClientConfiguration;
import com.cloudera.altus.client.AltusClientConfigurationBuilder;
import com.cloudera.altus.dataeng.api.DataengClient;
import com.cloudera.altus.dataeng.api.DataengClientBuilder;
import com.cloudera.altus.dataeng.model.CreateAWSClusterRequest;
import com.cloudera.altus.dataeng.model.CreateAWSClusterResponse;
import com.cloudera.altus.dataeng.model.DeleteClusterRequest;
import com.cloudera.altus.dataeng.model.DeleteClusterResponse;
import com.cloudera.altus.dataeng.model.DescribeClusterRequest;
import com.cloudera.altus.dataeng.model.DescribeJobRequest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;

import org.ini4j.Wini;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


abstract class BaseIntegration {

	private static final Logger LOG = LoggerFactory.getLogger(BaseIntegration.class);

	private Wini ini = null;

	public Wini getIniFile() throws IOException {
		if (ini == null) {
		  InputStream in = getClass().getClassLoader().getResourceAsStream("SampleResources.ini");
		  if (in == null) {
		    LOG.error("Unable to read SampleResources.ini file");
		    throw new RuntimeException("Unable to load SampleResources.ini file ");
      }
      ini = new Wini(in);
		}
		return ini;
	}

	/**
	 * Creates a DataengClient that's used to communicate to the backend services.
	 * This is agnostic to the cloud provider.
	 * @return Dataengclient
	 */
	DataengClient createClient() {
		try {

		  /* By default, it will use the default credentials in the ~/.altus directory.
		  You can also pass in the credential provider by specifying an AltusCredentialsProvider object
		  in the withCredentials method.
		  EX: DataengClientBuilder.defaultBuilder().withClientConfiguration(altusClientConfiguration)
		  .withCredentials(altusCredentialsProvider)*/

			Wini ini = getIniFile();
			/* Application name denotes who is creating the cluster functions. */
			AltusClientConfiguration altusClientConfiguration = AltusClientConfigurationBuilder.defaultBuilder()
					.withClientApplicationName(ini.get("client", "clientApplicationName"))
					.build();
			/* Creates the DataengClient based on the default values other than the application name passed in. */
			return DataengClientBuilder.defaultBuilder()
					.withClientConfiguration(altusClientConfiguration)
					.build();
		}	catch (IOException ioe) {
				LOG.error("Unable to load SampleResources.ini file " + ioe.getMessage());
				throw new RuntimeException(
				    "Unable to load SampleResources.ini file " + ioe.getMessage());
		} catch (AltusClientException ace) {
			LOG.error("Client error occurred while creating the Altus client" + ace.getMessage());
			throw ace;
		}
	}

	/**
	 * Creates an AWS Cluster. An AWS environment needs to be created before calling this function.
	 * Creating an AWS environment has specific features just for AWS.
	 * @param client					Used to communicate with the backend services
	 * @param clusterName			Name of the cluster to be created
	 * @param clusterType			Type of the cluster to be created
	 * @return ClusterStatus  Status of the cluster being created
	 */
	String createAWSCluster(DataengClient client, String clusterName, String clusterType) {
		try {
			Wini ini = getIniFile();

			 /* Create cluster with minimal input and use defaults (e.g. for ebs config) */
			CreateAWSClusterRequest request = new CreateAWSClusterRequest();
			request.setClusterName(clusterName);

			 /* Specify the CDH version to use such for CDH5.13, put CDH513 there */
			request.setCdhVersion(ini.get("AWSCluster", "cdhVersion"));

			String publicKey = ini.get("credentials", "ssh_public_key_location");
			String sshKey = getSshKeyContents(publicKey);

			request.setPublicKey(sshKey);

			request.setServiceType(clusterType);
			 /* Specify the AWS instance type such as m4.xlarge */
			request.setInstanceType(ini.get("AWSCluster", "instanceType"));
			request.setWorkersGroupSize(3);

			 /* Altus environment name */
			request.setEnvironmentName(ini.get("AWSCluster", "environmentName"));
			 /* Cloudera Manager username here */
			request.setClouderaManagerUsername(ini.get("AWSCluster", "CMUsername"));
			 /* Cloudera Manager password here */
			request.setClouderaManagerPassword(ini.get("AWSCluster", "CMPassword"));

			CreateAWSClusterResponse response = client.createAWSCluster(request);
			return pollClusterStatus(client, response.getCluster().getClusterName());
		} catch (IOException ioe) {
			LOG.error("Unable to load SampleResources.ini file " + ioe.getMessage());
			throw new RuntimeException(
			    "Unable to load SampleResources.ini file " + ioe.getMessage());
		} catch (AltusServiceException ase) {
			LOG.error(
			    "Altus exception occurred when creating the cluster "
              + " http code " + ase.getHttpCode() + " : "
              + ase.getMessage());
			throw ase;
		}
	}

	/**
	 * Poll the cluster to see if it was created successfully
	 * @param client				DataengClient used for polling the cluster
	 * @param clusterName		Cluster to poll
	 * @return ClusterStatus	Final status of the cluster
	 */
	String pollClusterStatus(DataengClient client, String clusterName) {

	  /* Poll the cluster to determine if cluster was created successfully */
		DescribeClusterRequest describeClusterRequest = new DescribeClusterRequest();
		describeClusterRequest.setClusterName(clusterName);

		while (true) {
			String clusterStatus = client.describeCluster(describeClusterRequest).getCluster().getStatus();
			if ("CREATED".equals(clusterStatus)) {
				LOG.info("Successfully created AWS cluster " + clusterName);
				return clusterStatus;
			} else if ("FAILED".equals(clusterStatus)
					|| "TERMINATING".equals(clusterStatus)) {
				LOG.error("AWS cluster " + clusterName + " was unable to get created. "
											+ " Cluster status is " + clusterStatus);
				return clusterStatus;
			} else {
				try {
					Thread.sleep(Duration.ofMinutes(1).toMillis());
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	/**
	 * Deletes the cluster. This is agnostic to the cloud provider.
	 * @param client        Client used to communicate to backend services
	 * @param clusterName   Name of cluster to delete
	 * @return Status code  Code of 200 represents that successful cluster deletion is in progress.
	 */
	int deleteCluster(DataengClient client, String clusterName) {
		DeleteClusterRequest deleteClusterRequest = new DeleteClusterRequest();
		deleteClusterRequest.setClusterName(clusterName);

		/* Throws an AltusServiceException (RuntimeException) on failure */
		DeleteClusterResponse response = client.deleteCluster(deleteClusterRequest);
		return response.getHttpCode();
	}

	/**
	 * Used to create a job on the Altus cluster. This is agnostic to the cloud provider.
	 * Throws an AltusServiceException (RuntimeException) on failure
	 * @param client								Client used to communicate to backend services
	 * @param clusterName						Name of cluster used to create jobs
	 * @return SubmitJobsResponse   Jobs response object which contains job status code and list of jobs on the cluster
	 */
	abstract void createJob(DataengClient client, String clusterName);

	/**
	 * Poll the job to see if it was created successfully
	 * @param client		DataengClient used for polling the job status
	 * @param jobId		Job Id to poll
	 * @return JobStatus	Final status of the job
	 */
	String pollJobStatus(DataengClient client, String jobId) {

	  /* Poll the job to determine if cluster was created successfully */
		DescribeJobRequest jobRequest = new DescribeJobRequest();
		jobRequest.setJobId(jobId);

		while (true) {
			String currentJobStatus = client.describeJob(jobRequest).getJob().getStatus();
			if ("COMPLETED".equals(currentJobStatus)) {
				LOG.info("Successfully completed job ");
				return currentJobStatus;
			} else if ("FAILED".equals(currentJobStatus)
					|| "TERMINATING".equals(currentJobStatus)) {
				LOG.error("Job " + jobId + currentJobStatus.toString());
				return currentJobStatus;
			} else {
				try {
					Thread.sleep(Duration.ofSeconds(30).toMillis());
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	/**
	 * Reads in the key specified in the file. More information can be found
	 * in the "Creating and Working with Clusters on the Console"
	 # section of the Altus documentation.
	 * @param file						file containing the private key
	 * @return String					private key
	 * @throws IOException		Occurs when their is an issue reading in the file.
	 */
	String getSshKeyContents(String file) throws IOException{
		return new String(Files.readAllBytes(Paths.get(file)));
	}
}
