FROM ubuntu:22.04
ARG DEBIAN_FRONTEND=noninteractive
RUN export DEBIAN_FRONTEND=noninteractive
RUN apt update  
RUN apt upgrade -y   
RUN apt install sudo curl gnupg2 systemctl -y
RUN sudo apt install -y postgresql-common
RUN sudo yes '' | /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
RUN apt install build-essential cmake libproj-dev libjson-c-dev libprotobuf-c-dev postgresql-16 git postgresql-server-dev-16 libgsl-dev -y   
RUN sudo service postgresql start  
RUN sudo apt install libgeos++-dev libgeos3.10.2 libgeos-c1v5 libgeos-dev libgeos-doc postgresql-16-postgis-3 -y  
RUN git clone https://github.com/MobilityDB/MobilityDB  
RUN mkdir MobilityDB/build && \
cd MobilityDB/build && \
cmake .. && \
make && \
sudo make install  
RUN curl https://install.citusdata.com/community/deb.sh | sudo bash
# install the server and initialize db
RUN sudo apt-get -y install postgresql-16-citus-12.1
RUN echo "shared_preload_libraries = 'citus, postgis-3'" >> /etc/postgresql/16/main/postgresql.conf  
RUN echo "max_locks_per_transaction = 128" >> /etc/postgresql/16/main/postgresql.conf  
RUN echo "listen_addresses = '*'" >> /etc/postgresql/16/main/postgresql.conf  
RUN echo "# TYPE DATABASE USER CIDR-ADDRESS  METHOD" >> /etc/postgresql/16/main/pg_hba.conf
RUN echo "host    all             all             0.0.0.0/0            trust" >> /etc/postgresql/16/main/pg_hba.conf 
RUN echo "host    all             all             127.18.0.0/32        trust" >> /etc/postgresql/16/main/pg_hba.conf 

# Also allow the host unrestricted access to connect to itself
RUN echo "host    all             all             127.0.0.1/32          trust" >> /etc/postgresql/16/main/pg_hba.conf 
RUN echo "host    all             all             ::1/128               trust" >> /etc/postgresql/16/main/pg_hba.conf 
RUN sudo /etc/init.d/postgresql restart && \
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'test';" && \
PGPASSWORD=test sudo -u postgres psql -c "CREATE EXTENSION PostGIS;" && \
PGPASSWORD=test sudo -u postgres psql -c "CREATE EXTENSION MobilityDB;" && \
PGPASSWORD=test sudo -u postgres psql -c "CREATE EXTENSION citus;" && \
sudo service postgresql restart
# and make it start automatically when computer does
RUN sudo update-rc.d postgresql enable
# citus coordinator node setup, with worker nodes being found at 172.18.0.*, where * is 3 through 7.


