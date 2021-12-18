#/bin/bash

apt-get update -y
apt autoremove
apt-get install -y libmysqlclient-dev libssl-dev comerr-dev libsasl2-dev
apt-get install -y python3 python3-pip python-dev python3-psycopg2
wget https://repo.anaconda.com/archive/Anaconda3-2021.11-Linux-x86_64.sh
chmod a+x Anaconda3-2021.11-Linux-x86_64.sh
bash ./Anaconda3-2021.11-Linux-x86_64.sh -b | yes
eval "$(/root/anaconda3/bin/conda shell.bash hook)"
conda create -y -n airflow pip setuptools python=3.6
conda activate airflow
mkdir -p /root/airflow
export AIRFLOW_HOME=/root/airflow
export SLUGIFY_USES_TEXT_UNIDECODE=yes
pip install -r ../src/requirements.txt
