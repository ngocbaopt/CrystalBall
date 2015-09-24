$ hadoop fs -rm -r /user/cloudera/crystalball
$ hadoop fs -rm -r /user/cloudera/crystalball/input
$ hadoop fs -rm -r /user/cloudera/crystalball/output

$ sudo su hdfs
$ hadoop fs -mkdir /user/cloudera
$ hadoop fs -chown cloudera /user/cloudera
$ exit
$ sudo su cloudera
$ hadoop fs -mkdir /user/cloudera/crystalball /user/cloudera/crystalball/input

$ echo "34 56 29 12 34 56 92 10 34 12" > user1
$ echo "18 29 12 34 79 18 56 12 34 92" > user2
$ echo "26 56 28 39 46 16 56 28 9 46" > user3
$ hadoop fs -put user* /user/cloudera/crystalball/input

$ mkdir -p build
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* ./build/*.java -d build -Xlint

$ jar -cvf stripes.jar -C build/ .

$ hadoop jar stripes.jar bigdata.project.stripes.StripesMain /user/cloudera/crystalball/input /user/cloudera/crystalball/output

$ hadoop fs -cat /user/cloudera/crystalball/output/* 