#!/bin/bash

# User should be superuser or user id should be 1001

# User is root 
sudo mkdir -p kafka
sudo chmod -R g+rwX kafka
mkdir -p kafka-ui
touch kafka-ui/config.yml
cat helpers/config.yml > kafka-ui/config.yml

# # User with id 1001 (uncomment the chown command)
# mkdir -p kafka
# chmod -R g+rwX kafka
# # chown -R 1001:1001 kafka
# mkdir -p kafka-ui
# touch kafka-ui/config.yml
# cat helpers/config.yml > kafka-ui/config.yml