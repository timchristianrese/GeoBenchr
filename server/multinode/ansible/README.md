# Installation of SUTs on machines
This ansible playbook installs the SUTs on a 5-node cluster. To do so, several configurations must be made by the benchmark user in order to guarantee a proper setup. 
Ansible needs to be installed along with Python on the machine from where the playbook is run, and an Ansible user needs to be configured on the machines where the system(s) need to be installed.  
Additionally, the machine names need to be adjusted in the `inventory/hosts.yml` file.  
This playbook requires several files to be downloaded before installation. Currently, the playbook is setup to download the required files from the internet to the SUT machines. There is however also the possibility to upload them from the local machine (commented out in the relevant playbooks).  
To install all SUTs on the machine, please run the following: 
``` 
ansible-playbook -i inventory/hosts.yml playbooks/all.yml -v 
```
Just Sedona: 
```
ansible-playbook -i inventory/hosts.yml playbooks/hadoop.yml -v
ansible-playbook -i inventory/hosts.yml playbooks/spark.yml -v 
ansible-playbook -i inventory/hosts.yml playbooks/sedona.yml -v 
```

Just MobilityDB/PostGIS:
```
ansible-playbook -i inventory/hosts.yml playbooks/postgres.yml -v 
ansible-playbook -i inventory/hosts.yml playbooks/citus.yml -v 
ansible-playbook -i inventory/hosts.yml playbooks/mobilitydb.yml -v 
```
