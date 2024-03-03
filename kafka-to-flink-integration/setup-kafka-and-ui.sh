#!/bin/bash

# Root must be the owner of the kafka folder 
sudo mkdir -p kafka
sudo chmod -R g+rwX kafka
mkdir -p kafka-ui
touch kafka-ui/config.yml
cat config.yml > kafka-ui/config.yml