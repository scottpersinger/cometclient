#!/bin/bash
export CLASSPATH=$CLASSPATH:./bayeux-api-2.3.1.jar:./cometd-java-client-2.3.1.jar:./cometd-java-common-2.3.1.jar:./jetty-client-7.4.4.v20110707.jar:./jetty-http-7.4.4.v20110707.jar:./jetty-io-7.4.4.v20110707.jar:./jetty-util-7.4.4.v20110707.jar:./bin
echo $CLASSPATH
echo "Usage: StreamingEventEcho <username> <password:token> <topic1> <topic2> ..."
java demo.StreamingEventEcho $@
