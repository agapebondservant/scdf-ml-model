#!/bin/bash

mkdir -p git_tmp

git clone $GIT_SYNC_REPO git_tmp

mv git_tmp/* .

ls -ltr .

python -m $MODEL_ENTRY