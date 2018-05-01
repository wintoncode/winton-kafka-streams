# -*- mode: ruby -*-
# vi: set ft=ruby :
Vagrant.configure("2") do |config|

  config.vm.box = "bento/ubuntu-16.04"

  config.vm.network "forwarded_port", guest: 2181, host: 2181
  config.vm.network "forwarded_port", guest: 9092, host: 9092

  config.vm.provider "virtualbox" do |v|
    v.memory = 2048
    v.cpus = 2
  end

  config.vm.provision "shell", inline: <<-SHELL
    export SCALA_VER=2.11
    export KAFKA_VER=1.0.0
    export KAFKA_PACKAGE=kafka_${SCALA_VER}-${KAFKA_VER}

    apt-get update
    apt-get install -y tmux htop vim wget git

    apt-get install -y build-essential software-properties-common python-software-properties
    wget -qO - http://packages.confluent.io/deb/3.3/archive.key | apt-key add -
    add-apt-repository "deb [arch=amd64] http://packages.confluent.io/deb/3.3 stable main"
    apt-get update
    apt-get install -y librdkafka-dev

    wget -q https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
    sh Miniconda3-latest-Linux-x86_64.sh -b -f -p /home/vagrant/miniconda3
    rm -f Miniconda3-latest-Linux-x86_64.sh
    /home/vagrant/miniconda3/bin/conda create -q -y -n vagrant python=3.6
    echo PATH=/home/vagrant/miniconda3/bin:\$PATH >> /home/vagrant/.profile
    echo source activate vagrant >> /home/vagrant/.profile
    echo cd /vagrant/ >> /home/vagrant/.profile

    apt-get install -y zookeeperd openjdk-8-jdk kafkacat
    wget -q http://mirror.ox.ac.uk/sites/rsync.apache.org/kafka/${KAFKA_VER}/${KAFKA_PACKAGE}.tgz
    tar -xzf ${KAFKA_PACKAGE}.tgz
    rm -f ${KAFKA_PACKAGE}.tgz
    mv ${KAFKA_PACKAGE} /opt/kafka
  SHELL

  config.vm.provision "shell", run: "always", inline: <<-SHELL
    /home/vagrant/miniconda3/envs/vagrant/bin/pip install -e /vagrant/.[develop]
    chown -R vagrant:vagrant /home/vagrant/miniconda3
    rm -fr /tmp/kafka*
    nohup /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties > /tmp/kafka.log 2>&1 &
  SHELL
end
