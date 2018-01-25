Altus Java SDK Sample

This is a sample that demonstrates how to connect to Altus service to make basic requests using the Altus Java SDK. The sample code is based on the tutorials mentioned in the Cloudera documentation (https://www.cloudera.com/documentation/altus/topics/altaws_tut_medicare.html#tut_create_cluster_jobs_spark).

Prerequisites:
1) Must have a valid Altus account. Please refer to the "Getting Started as an Altus User page" (https://www.cloudera.com/documentation/altus/topics/ag_dataengr_overview.html).
2) Make sure you have a valid account associated with the Cloud provider per Altus requirements. This sample code uses AWS, however the same APIs can be used with other Cloud Providers that Altus supports such as Azure.
3) Have downloaded the Altus-SDK-Java jar file.  Logging jar file is not included in the Altus-SDK-Java jar to allow users the ability to use their own logging implementation.

Running the Sample:
1) Setup the Maven project.
2) Replace the missing values in the SampleResources.ini file such as environmentName, outputLocation.