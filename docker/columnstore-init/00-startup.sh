#!/bin/bash
chown -R mysql:mysql /var/lib/columnstore
chmod -R 777 /var/lib/columnstore
controllernode fg &
sleep 8
PrimProc &
sleep 3
WriteEngineServer &
sleep 3
DMLProc &
sleep 3
DDLProc &
sleep 5
dbbuilder 7
sleep 3
mcs-savebrm.py
