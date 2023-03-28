#!/bin/bash

# Run this to add GPU configuration to the services in docker-compose.yml
yq -i '. *= load("gpu-snippet.yml")' docker-compose.yml
