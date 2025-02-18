#!/bin/bash

<<<<<<< HEAD
# User should be superuser or user id should be 1001

# User is root 
=======
# Root must be the owner of the kafka folder 
>>>>>>> 0dd3df2 (Folder to store parsed events of the sampled files)
sudo mkdir -p kafka
sudo chmod -R g+rwX kafka
mkdir -p kafka-ui
touch kafka-ui/config.yml
cat helpers/config.yml > kafka-ui/config.yml

<<<<<<< HEAD
# # User with id 1001 (uncomment the chown command)
# mkdir -p kafka
# chmod -R g+rwX kafka
# # chown -R 1001:1001 kafka
=======
# # Alternative with non root user
# mkdir -p kafka
# chmod -R g+rwX kafka
# chown -R 1001:1001 kafka
>>>>>>> 0dd3df2 (Folder to store parsed events of the sampled files)
# mkdir -p kafka-ui
# touch kafka-ui/config.yml
# cat helpers/config.yml > kafka-ui/config.yml