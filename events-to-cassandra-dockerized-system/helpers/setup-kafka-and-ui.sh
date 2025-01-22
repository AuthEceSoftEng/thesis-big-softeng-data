#!/bin/bash

# Root must be the owner of the kafka folder 
sudo mkdir -p kafka
sudo chmod -R g+rwX kafka
mkdir -p kafka-ui
touch kafka-ui/config.yml
cat helpers/config.yml > kafka-ui/config.yml

# # Alternative with non root user
# mkdir -p kafka
# chmod -R g+rwX kafka
# # chown -R 1001:1001 kafka
# mkdir -p kafka-ui
# touch kafka-ui/config.yml
# cat helpers/config.yml > kafka-ui/config.yml