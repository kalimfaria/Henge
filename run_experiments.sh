a=0

while [ $a -lt 16 ]
do


	echo $a
	cd /tmp
	sudo rm *.log
	cd /var/zookeeper
	sudo rm -r *

	sudo killall java
	cd /var/stela/zookeeper-3.4.6
	sudo bin/zkServer.sh start

	cd /var/nimbus/storm/lib
	sudo mv advanced-stela-0.10.1-SNAPSHOT-jar-with-dependencies-btbv-with-logs.jar advanced-stela-0.10.1-SNAPSHOT-jar-with-dependencies.jar
	cd ..
	sudo bin/storm nimbus &
	sudo bin/storm ui &
	sleep 30s
	sudo bin/storm jar examples/storm-starter/storm-starter-0*.jar storm.starter.PageLoadTopology production-topology1 remote
	sudo bin/storm jar examples/storm-starter/storm-starter-0*.jar storm.starter.PageLoadTopology_LessSLO production-topology2 remote
	sudo bin/storm jar examples/storm-starter/storm-starter-0*.jar storm.starter.ProcessingTopology production-topology3 remote
	sudo bin/storm jar examples/storm-starter/storm-starter-0*.jar storm.starter.ProcessingTopology_LessSLO production-topology4 remote
	sleep 30m
	sudo bin/storm kill production-topology1
	sudo bin/storm kill production-topology2
	sudo bin/storm kill production-topology3
	sudo bin/storm kill production-topology4
	foldername=$(date +%Y-%m-%d-%T)+"BTBV"
	logname=$(date +%Y-%m-%d-%T)+"BTBV.log"
	mkdir -p ~/henge-experiments/HengeResults/"$foldername"
	sudo cp /tmp/*.log ~/henge-experiments/HengeResults/"$foldername"
	sudo cp /var/nimbus/storm/logs/*.log ~/henge-experiments/HengeResults/"$foldername"
	sudo mv ~/henge-experiments/HengeResults/"$foldername"/output.log ~/henge-experiments/HengeResults/"$foldername"/"$logname"


	cd lib
	sudo mv advanced-stela-0.10.1-SNAPSHOT-jar-with-dependencies.jar  advanced-stela-0.10.1-SNAPSHOT-jar-with-dependencies-btbv-with-logs.jar

	a=`expr $a + 1`
done
