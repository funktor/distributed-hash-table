import sys, os
import mmh3

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print ("Correct usage: cluster_id script addresses")
        exit()

    cluster_id = str(sys.argv[1])
    script = str(sys.argv[2])
    addresses = eval(str(sys.argv[3]))
    ips = [x.split(":")[0] for x in addresses]
    ports = [x.split(":")[1] for x in addresses]
    
    for i in range(len(ips)):
        service_id = str(mmh3.hash(cluster_id + str(i), signed=False))
        os.system(f"start \"{script} {service_id}-{cluster_id}-{ips[i]}-{ports[i]}\" python \"{script}\" \"{service_id}\" \"{cluster_id}\" \"{ips[i]}\" \"{ports[i]}\"")
