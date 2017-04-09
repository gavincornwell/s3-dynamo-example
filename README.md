# Welcome
This repository contains a very simple example of triggering AWS services via events.

There is some client code that exercises the S3 APIs and two lambda functions.

The ContentAddedHandler function can be used to handle S3 put events.
It adds an entry to a DynamoDB table.

The NodeAddedHandler function can be used to handle Dynamo events.

Deployment and configuration of the functions is left as an exercise for the reader.
