#!/bin/bash
# Copyright © 2018. TIBCO Software Inc.
#
# This file is subject to the license terms contained
# in the license file that is distributed with this file.

# set AWS environment for a specified $ENV_NAME and $AWS_REGION
# usage: source env.sh profile
# specify profile if aws user assume a role of a different account, the assumed role should be defined in ~/.aws/config
# you may also set AWS_PROFILE=your_profile, and do not pass any variables to this script to use default config

export AWS_CLI_HOME=${HOME}/.aws

##### usually you do not need to modify parameters below this line

# return the full path of this script
function getScriptDir {
  local src="${BASH_SOURCE[0]}"
  while [ -h "$src" ]; do
    local dir ="$( cd -P "$( dirname "$src" )" && pwd )"
    src="$( readlink "$src" )"
    [[ $src != /* ]] && src="$dir/$src"
  done
  cd -P "$( dirname "$src" )" 
  pwd
}

# managed ethereum node is in this region
export AWS_REGION=us-east-1
if [[ ! -z "${1}" ]]; then
  export AWS_PROFILE=${1}
fi

export AWS_ZONES=${AWS_REGION}a,${AWS_REGION}b,${AWS_REGION}c

export SCRIPT_HOME=$(getScriptDir)
export KEYFILE=eth-track
export KEYNAME=${KEYFILE}-keypair
export SSH_PUBKEY=${SCRIPT_HOME}/config/${KEYFILE}.pub
export SSH_PRIVKEY=${SCRIPT_HOME}/config/${KEYFILE}.pem
export APP_HOST=ec2-34-201-218-202.compute-1.amazonaws.com

if [ ! -f ${SSH_PRIVKEY} ]; then
  mkdir -p ${SCRIPT_HOME}/config
fi
