import os
import sys
#Experiment Variables

eiger1 = "104.236.140.240"
eiger2 = "188.226.251.145"
eiger3 = "104.236.191.32"
eiger4 = "192.241.215.97"
eiger5 = "104.236.152.144"

DC0 = [eiger1,eiger3,eiger4,eiger5] #add nodes here
DC1 = [eiger2]
NODES = DC0 + DC1
ALL_NODES = [eiger1,eiger2,eiger3,eiger4,eiger5]

VAL_SIZES = ["10"] #bytes
LATENCY_MAX = ["0"] #deviation of normally distributed simulated mutation delay
RATIO_WRITES = ["0.3","0.1","0.01"] #writes/read
#LATENCY_MAX = ["0"] #ms
#RATIO_WRITES = ["0.125"] #writes/read


NUM_OPERATIONS = 10000
TEST_TYPE = "regular-stress" #facebook-stress | regular-stress
FACEBOOK_USE_EIGER = "no" # yes | no (default no)
NUM_USERS_FACEBOOK = 20

base_dir = "regular-results-num-nodes" #where the output will go

def mkdir(path):
    os.system("mkdir {}".format(path))

def experiment():
    print("Generating Files")

    NUM_NODES = len(DC0)

    mkdir(base_dir)

    base_path_node = "{}/{}_node".format(base_dir,NUM_NODES)
    mkdir(base_path_node)
    for val in VAL_SIZES:
        base_path_val = "{}/{}_val".format(base_path_node, val)
        mkdir(base_path_val)
        for latency in LATENCY_MAX:
            base_path_lat = "{}/{}_lat".format(base_path_val, latency)
            mkdir(base_path_lat)
            for ratio in RATIO_WRITES:
                base_path_rat = "{}/{}_write_ratio".format(base_path_lat, ratio)
                mkdir(base_path_rat)
                perform_experiment(val, latency, ratio)
                files = []
                for indx, address in enumerate(DC0):
                    filename = "{}/eiger{}.log".format(base_path_rat,indx)
                    files.append(filename)
                    copy_logs(address, filename)
                generate_report(files, "{}/report.txt".format(base_path_rat),  "{}/report.csv".format(base_dir), ratio, latency, val, NUM_NODES)


def reset_nodes(latency):
    print("Reseting all nodes....")
    for node in ALL_NODES:
        cmd = "sshpass -p $eiger_pass ssh eiger@{} 'cd eiger; bash deegan_burn_it_all.bash;'".format(node)
        print(cmd)
        os.system(cmd)
    for node in NODES:
        cmd = "sshpass -p $eiger_pass ssh eiger@{} 'cd eiger; bash deegan_datacenter_launcher.bash {} skip'".format(node, latency)
        if node == NODES[-1]:
            #last node
            cmd = "sshpass -p $eiger_pass ssh eiger@{} 'cd eiger; bash deegan_datacenter_launcher.bash {}'".format(node, latency)
        print(cmd)
        os.system(cmd)

def perform_experiment(val, latency, ratio):
    print("Performing Experiment VAL:{} LAT:{} RAT:{}".format(val, latency, ratio))
    reset_nodes(latency)
    print("Running Client Stress Tests on {}".format(eiger2))
    cmd = "sshpass -p $eiger_pass ssh eiger@{} 'cd eiger; export testType={}; export useEiger={}; export num_facebook_users={}; export num_operations={}; export chance_of_write={}; export value_size={}; export CASSANDRA_HOME=/home/eiger/eiger; env; ./deegan_client_launcher.bash > out.log'".format(eiger2, TEST_TYPE, FACEBOOK_USE_EIGER, NUM_USERS_FACEBOOK, NUM_OPERATIONS, ratio, val)
    print(cmd)
    os.system(cmd)


def copy_logs(address, path):
    print("Copying logs from {} to {}".format(address, path))
    os.system("sshpass -p $eiger_pass scp eiger@{}:/home/eiger/eiger/cassandra_var/cassandra_system.0.log {}".format(address,path));

def generate_report(input_files, output_path, csv_path, ratio, latency, val, num_nodes):
    print("Generating report...")
    print(input_files)
    print(output_path)
    f = open(output_path, "w")
    f.write("Write/Read ratio: {}\n".format(ratio))
    f.write("Latency: {}ms\n".format(latency))
    f.write("Value Size: {}b\n\n".format(val))
    f.write("Total Operiations: {}b\n\n".format(NUM_OPERATIONS))
    all_numbers = []
    for input_file in input_files:
        f.write("\n\n{}:\n\n".format(input_file))
        numbers = []
        for line in open(input_file):
            if("Completion Complete" in line):
                i = line.index("e (") + 3
                j = line.index("ms")
                numString = line[i:j]
                numbers.append(int(numString))
        print numbers
        if(len(numbers) == 0):
            continue
        all_numbers = all_numbers + numbers
        total = sum(numbers)
        average = total/len(numbers)
        median = sorted(numbers)[len(numbers)/2]
        f.write("Average: {}ms\n".format(average))
        f.write("Median: {}ms\n".format(median))
        f.write(str(numbers))
    f.close()

    csv = open(csv_path, "a")
    csv.write(str(num_nodes) + ',' + str(val) + ',' + str(latency) + ',' +str(ratio) + ','.join([str(x) for x in all_numbers]) + '\n')


if __name__ == "__main__":
    if(len(sys.argv) != 2):
        print("Usage: runner.py (reset | experiment)")
    elif(sys.argv[1] == "reset"):
        reset_nodes(0)
    elif(sys.argv[1] == "experiment"):
        experiment()
    else:
        print("Usage: runner.py (reset | experiment)")

