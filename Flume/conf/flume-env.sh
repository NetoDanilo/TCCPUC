#PATH do JAVA_HOME.
export JAVA_HOME=/opt/jdk1.8.0_202
#OPÇÕES do JAVA para resolver o erro GC overhead limit exceeded error in flume channel
#https://community.cloudera.com/t5/Support-Questions/Flume-GC-overhead-limit-exceeded-error-in-flume-channel/td-p/28724
export JAVA_OPTS="-Xms100m -Xmx2000m -Dcom.sun.management.jmxremote"

#AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY para se conectar ao AWS S3.
export AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXXXXX
export AWS_SECRET_ACCESS_KEY=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
