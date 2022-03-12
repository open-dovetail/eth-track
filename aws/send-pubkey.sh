#!/bin/sh

# send newly generated pubkey to an EC2 instance
# which is required after generating new key-pair, then 
# ssh -i ./config/eth-track.pem ec2-user@34.201.218.202
source ./env.sh oocto

aws ec2-instance-connect send-ssh-public-key \
    --instance-id i-0dece64666cc9da9b \
    --availability-zone us-east-1a \
    --instance-os-user ec2-user \
    --ssh-public-key file://config/${KEYFILE}.pub
