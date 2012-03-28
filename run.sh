#!/bin/bash
export CLASSPATH=$CLASSPATH:./bayeux-api-2.3.1.jar:./cometd-java-client-2.3.1.jar:./cometd-java-common-2.3.1.jar:./jetty-client-7.4.4.v20110707.jar:./jetty-http-7.4.4.v20110707.jar:./jetty-io-7.4.4.v20110707.jar:./jetty-util-7.4.4.v20110707.jar:./bin
echo $CLASSPATH
java demo.StreamingEventEcho "scottpersinger@gmail.com" "101monkeyswNRPJF0vgZAWcaI83nqmt9ngc" cm.Contact cm.Account
