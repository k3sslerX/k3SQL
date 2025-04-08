#!/bin/bash

mkdir /opt/k3SQL
cd /opt/k3SQL
mkdir data
mkdir config
cd data
mkdir k3db
cd k3db
echo -e "3 name|3 password\nk3user|333"
