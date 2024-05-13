#!/bin/bash

sudo apt update
echo "Installing Python"

sudo apt install python --yes

python3 --version

echo "Installing pip"

sudo apt install python3-pip --yes
pip3 --version

SHELL_PROFILE="$HOME/.bashrc"

cd ../work/thesis/ml

echo "Pulling repo from github"
git clone https://github.com/Mylos97/Thesis-ML.git repo
#cd ./repo

echo "Installing python requirements"
python3 -m venv ./venv
./venv/bin/python3 -m pip install --upgrade setuptools
./venv/bin/python3 -m ensurepip --upgrade
./venv/bin/python3 -m pip install -r requirements.txt


