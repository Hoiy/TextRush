#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/..
rm -rf data
mkdir data
source env/bin/activate

LUIGI_CONFIG_PATH="$DIR/luigi.cfg" PYTHONPATH="$DIR/.." luigi --module prepare_data.pipeline PrepareTrainingData --local-scheduler
