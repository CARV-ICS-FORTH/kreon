#!/bin/bash
set -e
pip install gitlint
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

FILES=$(git diff-tree --no-commit-id --name-only -r "$CI_COMMIT_SHA" | awk '$1=$1' ORS=' ')

for i in $FILES; do
	echo "---------------------------------------------------------"
	echo -e "${RED} Checking file $i ${NC}"
	pre-commit run --files "$i"
	echo -e "${GREEN} File $i PASSED ${NC}"
done

gitlint
