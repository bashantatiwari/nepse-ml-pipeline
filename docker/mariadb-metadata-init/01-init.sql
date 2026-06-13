CREATE DATABASE IF NOT EXISTS airflow_db;
CREATE DATABASE IF NOT EXISTS nepse_mlops;
GRANT ALL PRIVILEGES ON airflow_db.* TO 'nepse_user'@'%';
GRANT ALL PRIVILEGES ON nepse_mlops.* TO 'nepse_user'@'%';
FLUSH PRIVILEGES;
