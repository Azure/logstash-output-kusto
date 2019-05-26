# Changelog

## 0.1.0

- Plugin created with the logstash plugin generator

## 0.1.6

- plugin published to the public. supports ingestion json events into a specific table-database (without dynamic routing currently)

## 0.1.7

- fixed app_key (password) bug, include 0.1.7 of the kusto-java-sdk to allow working through a proxy

## 0.2.0

- move to version 1.0.0-BETA-01 of azure-kusto-java sdk

## 0.3.0

- move to version 1.0.0-BETA-04 of azure-kusto-java sdk
- better support multiple kusto outputs running in parallel

## 0.4.0

- set 'client name for tracing' to identify usage of this plugin on Kusto logs