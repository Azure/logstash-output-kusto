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

# 1.0.0

- Use stable (2.1.2) version of the java sdk, and retrieve it from maven with bundler.
- Renamed `mapping` to `json_mapping` in order to clarify usage. `mapping` still remains as a deprecated parameter.  


# 1.0.5

- Use (3.1.3) version of the java sdk, and retrieve it from maven with bundler.
- Added support for `proxy_host` `proxy_port` `proxy_protocol` to support proxying ingestion to Kusto

# 2.0.0

- Use (5.0.2) version of the java sdk, and retrieve it from maven with bundler. Supports logstash 8.6 versions and up
- Upgrade to latest Java SDK fixes [CVE](https://github.com/advisories/GHSA-599f-7c49-w659) and addresses Issue#48