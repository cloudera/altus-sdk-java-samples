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
import com.cloudera.altus.client.AltusClientConfiguration;
import com.cloudera.altus.client.AltusClientConfigurationBuilder;
import com.cloudera.altus.dataeng.api.DataengClient;
import com.cloudera.altus.dataeng.api.DataengClientBuilder;
import com.cloudera.altus.dataeng.model.ListClustersRequest;
import com.cloudera.altus.dataeng.model.ListClustersResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ListClusters {

	private static final Logger LOG =
			LoggerFactory.getLogger(ListClusters.class);

	public static void main(String[] args) {
		list();
	}

	private static void list() {
		try {
			AltusClientConfiguration altusClientConfiguration = AltusClientConfigurationBuilder.defaultBuilder()
					.withClientApplicationName("TestPartner")
					.build();

			/*
			If no credentials are provided, it uses the default credentials which is to look at the
			credentials file in the ~/.altus directory
			*/
			DataengClientBuilder dataEngClientBuilder =
					DataengClientBuilder.defaultBuilder().withClientConfiguration(altusClientConfiguration);
			DataengClient client = dataEngClientBuilder.build();
			ListClustersRequest listClusterRequest = new ListClustersRequest();
			ListClustersResponse listClusterResp = client.listClusters(listClusterRequest);

			LOG.info("Found the following number of clusters: " + listClusterResp.getClusters().size());
			LOG.info("List of clusters: " + listClusterResp.toString());
		} catch (AltusServiceException ase) {
			LOG.error("Unable to list clusters " + ase.getMessage());
		}
	}
}
