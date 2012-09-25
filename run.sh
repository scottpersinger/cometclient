#!/bin/bash
export CLASSPATH=$CLASSPATH:./bayeux-api-2.3.1.jar:./cometd-java-client-2.3.1.jar:./cometd-java-common-2.3.1.jar:./jetty-client-7.4.4.v20110707.jar:./jetty-http-7.4.4.v20110707.jar:./jetty-io-7.4.4.v20110707.jar:./jetty-util-7.4.4.v20110707.jar:./mysql-connector-java-5.1.20-bin.jar:./gson-2.1.jar:./rabbitmq-client.jar:./bin
echo $CLASSPATH
echo "Usage: StreamingEventEcho <dburl> <rabbiturl> <clientId> <clientSecret>"
#java demo.StreamingEventEcho $@
java demo.StreamingEventEcho "jdbc:mysql://localhost/cloud5?user=root&password=" "amqp://guest@beta.cloudconnect.com:5672/test" "3MVG9rFJvQRVOvk7dcW8zRXD4lv7ZdT638O2Ti7hrGGo2Oa7iFCoyabLryNhGv_j2x9yo0Ea6.VmqzUR6LDgD" "7176140918132069100"
