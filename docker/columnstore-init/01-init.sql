CREATE DATABASE IF NOT EXISTS nepse_columnstore;
GRANT ALL PRIVILEGES ON nepse_columnstore.* TO 'nepse_user'@'%';
FLUSH PRIVILEGES;
