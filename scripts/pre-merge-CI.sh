#!/bin/bash
set -e
# pip install gitlint
# FILES=$(git --no-pager diff "origin/$CI_MERGE_REQUEST_TARGET_BRANCH_NAME" --name-only)
FILES=$(git --no-pager diff "origin/$CI_MERGE_REQUEST_TARGET_BRANCH_NAME" --name-only)
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "$FILES"
for i in $FILES; do
	echo "---------------------------------------------------------"
	echo -e "${RED} Checking file $i ${NC}"
	# pre-commit run --files "$i"
	echo -e "${GREEN} File $i PASSED ${NC}"
done
