import os

os.system("sshpass -p Bs81tu93 ssh eiger@104.236.140.240 'cd eiger; bash deegan_burn_it_all.bash; bash deegan_datacenter_launcher.bash'")

#    'source ~/.bashrc; echo $local_token; cd eiger; source ~/.bashrc; echo $CASSANDRA_HOME; bash deegan_burn_it_all.bash; export max_mutation_delay_ms=200; ./deegan_datacenter_launcher.bash'")
#os.system("echo $PATH")