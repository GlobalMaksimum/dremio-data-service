#!/bin/bash 

export DREMIO_USER=husnusensoy
export DREMIO_PASS=Dremio2023!

if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
fi

python rest.py $@