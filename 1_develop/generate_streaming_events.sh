#!/bin/#!/usr/bin/env bash
echo "Installing packages"
# Install modules
sh ./install_packages.sh

echo "Generating synthetic users"
# Generate 10 fake web site users
python3 user_generator.py --n=10

echo "Generating synthetic events"
use_lag=$1

if [ "$use_lag" = true ] ; then
    echo "Using lag"
    python3 streaming_event_generator.py --project_id=$(gcloud config get-value project) -t=source
else
    echo "Not using lag"
    python3 streaming_event_generator.py --project_id=$(gcloud config get-value project) -t=source -off=1. -on=0. -l=0
fi