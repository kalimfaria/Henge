cd /var/nimbus
sudo cp storm/conf/storm.yaml .
sudo cp ~/apache-storm-0.10.1-SNAPSHOT.tar.gz
sudo tar -xvf apache-storm-0.10.1-SNAPSHOT.tar.gz
sudo mv apache-storm-0.10.1-SNAPSHOT storm
sudo cp storm.yaml storm/conf/
sudo apt-get update
sudo apt-get install vim
sudo apt-get install curl
sudo apt-get install jq
sudo apt-get install tmux
