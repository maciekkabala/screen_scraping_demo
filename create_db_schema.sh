#!/bin/bash

db_name="lasiodora_parahybana"
user="mucha"
passwd="pajak"

 
mysql -u${user} -p${passwd} < create_db_schema.sql
