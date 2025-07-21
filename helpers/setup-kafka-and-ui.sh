#!/bin/bash

# User should be root (superuser) or should have id = 1001
# Uncomment option 1 or 2 if the user is root or has id 1001

# Option 1: User is root 
sudo mkdir -p kafka
sudo chmod -R g+rwX kafka
mkdir -p kafka-ui
touch kafka-ui/config.yml
cat helpers/config.yml > kafka-ui/config.yml

# # Option 2: User with id 1001 (uncomment the chown command)
# mkdir -p kafka
# chmod -R g+rwX kafka
# # chown -R 1001:1001 kafka
# mkdir -p kafka-ui
# touch kafka-ui/config.yml
# cat helpers/config.yml > kafka-ui/config.yml