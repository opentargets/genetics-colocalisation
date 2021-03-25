#!/usr/bin/env bash
#

set -euo pipefail

mkdir -p software
cd software

# Install Java
sudo apt install openjdk-8-jdk
echo export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.profile
. ~/.profile

# Install conda
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
bash miniconda.sh -b -p $HOME/miniconda
echo export PATH="$HOME/miniconda/bin:\$PATH" >> ~/.profile
. ~/.profile

# Install R
sudo add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu bionic-cran35/'
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9
sudo apt update
sudo apt-get update
sudo apt-get -y install r-base-core
sudo apt-get -y install libxml2-dev libssl-dev libcurl4-openssl-dev

# Install parallel
sudo apt install parallel

echo COMPLETE
