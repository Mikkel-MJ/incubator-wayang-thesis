#!/bin/bash

# Install ThesisML python
cd /var/www/html/wayang-plugins/wayang-ml/src/main/python
git clone https://github.com/mylos97/Thesis-ML.git python-ml
cd ./python-ml
python3.11 -m venv ./venv
./venv/bin/python3.11 --version
./venv/bin/python3.11 -m pip install -r requirements.txt
cd /var/www/html
