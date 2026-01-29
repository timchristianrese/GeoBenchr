# transfer all resouces (supporting datasets) to the server
server_ip=$1
server_user=$2
scp -r "../../benchmark/configuration/ais/data/*" ${server_user}@${server_ip}:/home/${server_user}/data/ais/resources/

scp -r "../../benchmark/configuration/aviation/data/*" ${server_user}@${server_ip}:/home/${server_user}/data/aviation/resources/

scp -r "../../benchmark/configuration/cycling/data/*" ${server_user}@${server_ip}:/home/${server_user}/data/cycling/resources/

