#!/bin/bash
set +e
chown -R mysql:mysql /var/lib/columnstore /var/log/mariadb/columnstore /var/lib/mysql 2>/dev/null || true
chmod -R 777 /var/lib/columnstore 2>/dev/null || true
# Always start fresh
rm -rf /var/lib/columnstore/data1/systemFiles/dbrm/*
su -s /bin/bash mysql -c "mariadbd --user=mysql --datadir=/var/lib/mysql &"
sleep 15
controllernode fg &
sleep 3
workernode DBRM_Worker1 &
sleep 8
PrimProc &
sleep 3
WriteEngineServer &
sleep 3
DMLProc &
sleep 3
DDLProc &
sleep 8
dbbuilder 7 || true
sleep 3
mcs-savebrm.py || true
# Init database users
mariadb -u root -pnepse_secure_password -e "
CREATE DATABASE IF NOT EXISTS nepse_columnstore;
DELETE FROM mysql.user WHERE user='';
CREATE USER IF NOT EXISTS 'nepse_user'@'%' IDENTIFIED BY 'nepse_secure_password';
CREATE USER IF NOT EXISTS 'nepse_user'@'localhost' IDENTIFIED BY 'nepse_secure_password';
GRANT ALL PRIVILEGES ON *.* TO 'nepse_user'@'%' WITH GRANT OPTION;
GRANT ALL PRIVILEGES ON *.* TO 'nepse_user'@'localhost' WITH GRANT OPTION;
FLUSH PRIVILEGES;
" 2>/dev/null || true
echo "ColumnStore ready"
# Keep container alive
echo "ColumnStore is running. Keeping container alive..."
tail -f /dev/null
