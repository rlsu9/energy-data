#!/bin/zsh

# ****** Install via apt ******
#
# Source: https://www.postgresql.org/download/linux/ubuntu/

# Create the file repository configuration:
sudo sh -c 'echo "deb [arch=amd64] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

# Import the repository signing key:
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -

# Update the package lists:
sudo apt-get update

# Install the latest version of PostgreSQL.
# If you want a specific version, use 'postgresql-12' or similar instead of 'postgresql':
sudo apt-get -y install postgresql

# ****** Post-Install setup ******

## Allow remote access
sudo sh -c "echo \"local\tall\tall\ttrust\" >> /etc/postgresql/14/main/pg_hba.conf"
sudo service postgresql restart

## Move data directory
sudo systemctl stop postgresql
sudo systemctl status postgresql | grep "Stopped" > /dev/null
[ $? -eq 0 ] || { echo "Failed to stop postgresql service. Aborting..."; exit 1; }

DST_DATA_DIR=/c3lab-migration/prod/postgresql
sudo rsync -av /var/lib/postgresql/14/main/ $DST_DATA_DIR
sudo mv /var/lib/postgresql/14/main /var/lib/postgresql/14/main.bak

sudo sh -c "echo \"data_directory = '$DST_DATA_DIR'\" >> /etc/postgresql/14/main/postgresql.conf"
sudo systemctl start postgresql
sudo systemctl status postgresql | grep "active" > /dev/null
[ $? -eq 0 ] || { echo "Failed to start postgresql service. Please manually check ..."; exit 1; }

# Change password
echo 'Use "\password username" command to change password in the prompt below'
sudo -u postgres psql

# Create database
sudo -u postgres createdb electricity-data
sudo -u postgres createuser -s "$USER"
