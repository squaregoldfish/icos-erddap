#!/bin/bash

erddapDir=$1

find $erddapDir/bigParent -type f -name "subscriptionsV1ArchivedAt*txt" -mtime +30 -exec rm -f {} \;
find $erddapDir/bigParent/logs -type f -name "emailLog*txt" -mtime +30 -exec rm -f {} \;
find $erddapDir/bigParent/logs -type f -name "logArchivedAt*txt" -mtime +30 -exec rm -f {} \;
find $erddapDir/bigParent/logs -type f -name "logPreviousArchivedAt*txt" -mtime +30 -exec rm -f {} \;
