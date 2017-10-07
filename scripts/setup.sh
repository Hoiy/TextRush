#!/bin/bash

echo Reinstalling python virtualenv...

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/..
rm -rf env
virtualenv -p python3 env
source env/bin/activate
pip3 install jupyter luigi keras tensorflow spacy
python -m spacy download en
