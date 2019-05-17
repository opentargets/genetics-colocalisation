#!/usr/bin/env bash
#

set -euo pipefail

mkdir -p software
cd software

# Install conda
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
bash miniconda.sh -b -p $HOME/miniconda
echo export PATH="$HOME/miniconda/bin:\$PATH" >> ~/.profile
. ~/.profile

# Install R
sudo add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu bionic-cran35/'
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9
sudo apt update
sudo apt-get update
sudo apt-get -y install r-base-core
sudo apt-get -y install libxml2-dev libssl-dev libcurl4-openssl-dev

# Install coloc - Need to open R as sudo and run these commands
# source("https://bioconductor.org/biocLite.R")
# biocLite("snpStats")
# install.packages('coloc')
# install.packages("tidyverse")

echo COMPLETE
