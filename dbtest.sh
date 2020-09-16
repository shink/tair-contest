#!/bin/bash

red='\e[91m'
green='\e[92m'
yellow='\e[93m'
magenta='\e[95m'
cyan='\e[96m'
none='\e[0m'

path=/root/tair-contest

set_per_thread=$1
get_per_thread=$2

[ -z $set_per_thread ] && set_per_thread=100000
[ -z $get_per_thread ] && get_per_thread=100000

echo -e "set_per_thread is ${yellow}$set_per_thread${none}"
echo -e "get_per_thread is ${yellow}$get_per_thread${none}"

cd $path

make

cd judge

rm -rf DB

chmod +x judge.sh

./judge.sh ../lib $set_per_thread $get_per_thread
