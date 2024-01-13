#!/bin/bash

# Grant recursive 777 permissions to folders and subfolders
chmod -R 777 ./*

# Run Docker Compose
sudo docker-compose up -d --build

