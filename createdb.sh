#!/bin/bash

db_name="lasiodora_parahybana"
user="mucha"
passwd="pajak"

Q1="CREATE DATABASE IF NOT EXISTS ${db_name};"
Q2="GRANT USAGE ON *.* TO ${user}@localhost IDENTIFIED BY '${passwd}';"
Q3="GRANT ALL PRIVILEGES ON ${db_name}.* TO ${user}@localhost;"
Q4="FLUSH PRIVILEGES;"
SQL="${Q1}${Q2}${Q3}${Q4}"
 
mysql -uroot -p -e "$SQL"