#!/bin/bash
set -euo pipefail
[[ "$MYSQL_USER" =~ ^[a-zA-Z0-9_]+$ ]] || { echo 'MYSQL_USER must be alphanumeric/underscore'; exit 1; }
MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -u root <<SQL
CREATE DATABASE IF NOT EXISTS olympic_results CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
GRANT ALL PRIVILEGES ON olympic_results.* TO '${MYSQL_USER}'@'%';
SQL
