# Cloudera Altus SDK for Java Samples

This is a set of simple sample applications that demonstrates how to connect to
Altus service to make basic requests using the Altus SDK. The sample code
is based on the tutorials mentioned in the
[Cloudera documentation](https://www.cloudera.com/documentation/altus/topics/altaws_tut_medicare.html#tut_create_cluster_jobs_spark).

## Prerequisites
1) Must have a valid Altus account. Please refer to
   [Getting Started as an Altus User](https://www.cloudera.com/documentation/altus/Shared/altus_usr_get_started.html).
2) Make sure you have a valid Altus Environment associated with your cloud
   provider per Altus requirements. These samples assume the use of an AWS
   environment. Please refer to [Altus Environment](https://www.cloudera.com/documentation/altus/topics/altaws_adm_environment.html).
3) Make sure you have the Altus credentials to run the project. Please refer to
   [Configure the Altus Client with the API Access Key](https://www.cloudera.com/documentation/altus/Shared/altus_usr_get_started.html#configure_client).
   The Altus credential provider chain
   [`DefaultCredentialProviderChain.java`](https://github.com/cloudera/altus-sdk-java/blob/master/src/main/java/com/cloudera/altus/authentication/credentials/DefaultCredentialProviderChain.java)
   looks for credentials in this order:
    1) EnvironmentVariables: [`AltusEnvironmentVariableCredentialsProvider.java`](https://github.com/cloudera/altus-sdk-java/blob/master/src/main/java/com/cloudera/altus/authentication/credentials/AltusEnvironmentVariableCredentialsProvider.java)
    2) Java System properties [`AltusSystemPropertiesCredentialsProvider.java`](https://github.com/cloudera/altus-sdk-java/blob/master/src/main/java/com/cloudera/altus/authentication/credentials/AltusSystemPropertiesCredentialsProvider.java)
    3) Altus credentials file: [`AltusProfileCredentialsProvider.java`](https://github.com/cloudera/altus-sdk-java/blob/master/src/main/java/com/cloudera/altus/authentication/credentials/AltusProfileCredentialsProvider.java)
4) **Java 1.8** or newer.
5) **Maven 3.5** or newer.

## Provided Samples

* [`ListClusters`](https://github.com/cloudera/altus-sdk-java-samples/blob/master/src/main/java/com/cloudera/altus/sdk/samples/ListClusters.java):
  List the data engineering clusters present in your Altus account.
* [`HiveIntegration`](https://github.com/cloudera/altus-sdk-java-samples/blob/master/src/main/java/com/cloudera/altus/sdk/samples/HiveIntegration.java):
  Run the data transformation using a Hive job.
* [`MapReduceIntegration`](https://github.com/cloudera/altus-sdk-java-samples/blob/master/src/main/java/com/cloudera/altus/sdk/samples/MapreduceIntegration.java):
  Run the data transformation using a MapReduce job.
* [`SparkIntegration`](https://github.com/cloudera/altus-sdk-java-samples/blob/master/src/main/java/com/cloudera/altus/sdk/samples/SparkIntegration.java):
  Run the data transformation using a Spark job.
* [`SparkAllInOneIntegration`](https://github.com/cloudera/altus-sdk-java-samples/blob/master/src/main/java/com/cloudera/altus/sdk/samples/SparkAllInOneIntegration.java):
  Run the data transformation using a Spark job and the Altus SDK's all-in-one
  API that defines the cluster and the job in a single call.

## Running the Samples

1) Checkout the source code
2) Replace the missing values in [`src/main/resources/SampleResources.ini`](https://github.com/cloudera/altus-sdk-java-samples/blob/master/src/main/resources/SampleResources.ini).
   file such as `environmentName`, `outputLocation`, `ssh_public_key_location`, etc.
3) Build the samples with maven:
   ```sh
   mvn clean install
   ```
4) Run the command from the `target` directory. SparkAllInOneIntegration is the default class so no need to specify class.
   ```sh
   java -jar altus-sdk-java-samplecode-0.1-SNAPSHOT.jar
   ```
5) To run other classes such as the HiveIntegration class from the `target` directory:
   ```sh
   java -cp altus-sdk-java-samplecode-0.1-SNAPSHOT.jar:lib/* com.cloudera.altus.sdk.samples.HiveIntegration
   ```