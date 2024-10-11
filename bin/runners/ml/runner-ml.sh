#!/bin/bash

sudo apt update
echo "Installing Python"

sudo apt install -y \
    build-essential \
    libssl-dev\
    libffi-dev \
    software-properties-common && \
    sudo add-apt-repository ppa:deadsnakes/ppa -y && \
    sudo apt install -y \
    python3.11 \
    python3.11-dev \
    python3-pip \
    python3.11-venv

python3.11 --version

echo "Installing pip"

sudo apt install python3-pip --yes
python3.11 -m pip --version

SHELL_PROFILE="$HOME/.bashrc"

cd /work/lsbo-paper/ml

echo "Pulling repo from github"
git clone https://github.com/Mylos97/Thesis-ML.git repo
cd ./repo

echo "Installing python requirements"
python3.11 -m venv ./venv
./venv/bin/python3.11 -m pip install -r requirements.txt


