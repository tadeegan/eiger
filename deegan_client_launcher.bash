CP=$CASSANDRA_HOME/deegan_client/build/classes/
CP=$CP:$CASSANDRA_HOME/build/classes/main
CP=$CP:$CASSANDRA_HOME/build/classes/thrift
CP=$CP:$CASSANDRA_HOME/build/lib/jars/*
CP=$CP:$CASSANDRA_HOME/lib/*

echo $CP
java -cp $CP deegan.TestClient