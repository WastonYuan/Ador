
M = 1024 * 1024
K = 1024
# setting the single node tps (Mtps)
ador = 0.392464969661734
aria = 0.379187888785116
docc = 0.193217781054191
calvin = 0.178428773452895
caracal = 0.465485689

network_delay = 0.5 # ms
batch_size = 5000.0 # *10 batch (each batch's request network cost is 0.5 * 2 / 10)
batch_cnt = 10.0

all_node_cnt = 8
# for aria

print("Ador\tAria\tDOCC\tCalvin\tCaracal")
for partition_cnt in range(1, all_node_cnt + 1):
    # ador
    ador_latency = batch_size / partition_cnt / (ador * M) * K + network_delay * 0.1
    ador_tps = batch_size / (ador_latency / K) / M

    # aria
    aria_latency = batch_size / partition_cnt / (aria * M) * K + network_delay * 4.1
    aria_tps = batch_size / (aria_latency / K) / M
    # print(batch_size / partition_cnt / (aria * M) * K)

     # docc
    docc_latency = batch_size / partition_cnt / (docc * M) * K + network_delay * 0.1
    docc_tps = batch_size / (docc_latency / K) / M

    # calvin
    calvin_latency = batch_size / partition_cnt / (calvin * M) * K + network_delay * 0.1
    calvin_tps = batch_size / (calvin_latency / K) / M

    # caracal
    caracal_latency = batch_size / partition_cnt / (caracal * M) * K + network_delay * 0.1
    caracal_tps = batch_size / (caracal_latency / K) / M


    print(ador_tps, aria_tps, docc_tps, calvin_tps, caracal_tps)

    





