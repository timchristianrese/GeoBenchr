#delete the accumulo from hdfs
/opt/accumulo/bin/accumulo-cluster stop
/opt/hadoop/bin/hdfs dfs -rm -r /accumulo
/opt/accumulo/bin/accumulo init --instance-name test -u root --password test
/opt/accumulo/bin/accumulo-cluster start