import random

tps = 0.392464969661734
net_eq_txn = 1
cross_rate = 0.00
batch_txn = 5000
random.seed( 10 )
# multi txn most cross two node
print("tps")
print(str(tps * batch_txn / (batch_txn + 2)))
cross_rates = [0.0, 0.01, 0.05, 0.1, 0.2]
for node_cnt in range(2, 9):

    txn_cross_node = 2 # less than node_cnt (the cross node is specific! and no random)
    
    cross_txn_cnt = int(batch_txn * cross_rate)
    avg_node_txn = int(batch_txn / node_cnt)

    corss_node_txn_cnt = avg_node_txn + int(cross_txn_cnt / 2)
    
    node_n_cross_txn = []
    for i in range(txn_cross_node):
        node_n_cross_txn.append([])
    for add_cross_txn in range(cross_txn_cnt):

        for j in range(txn_cross_node):
            node_j_corss_txn_id = random.randint(1, corss_node_txn_cnt)
            node_n_cross_txn[j].append(node_j_corss_txn_id)

    
    for j in range(txn_cross_node):
        node_n_cross_txn[j].sort()
    # print(node_cnt)
    # print(node_n_cross_txn)
    before_node = [0] * txn_cross_node
    run = [0] * txn_cross_node
    # before_0 = 0
    # before_1 = 0
    run_time_in_txn = 0
    for i in range(cross_txn_cnt): # nth cross txn
        for j in range(txn_cross_node): # nth node
            run[j] = node_n_cross_txn[j][i] - before_node[j]
            before_node[j] = node_n_cross_txn[j][i]
        run_time_in_txn += max(run)
        # print(max(run), run)
        # before_0 = node_0_cross_txn[i]
        # before_1 = node_1_cross_txn[i] 
    
    for j in range(txn_cross_node): # nth node
            run[j] = corss_node_txn_cnt - before_node[j]
            before_node[j] = corss_node_txn_cnt
    run_time_in_txn += max(run)

    net_add = cross_txn_cnt * (txn_cross_node - 1) * net_eq_txn
    run_time_in_txn += net_add

    print(str(batch_txn * tps / run_time_in_txn))
    # print(run_time_in_txn, avg_node_txn)