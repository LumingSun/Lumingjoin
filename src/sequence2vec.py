import json
import csv
from types import resolve_bases

# read sequences
seq_path = "/home/sunluming/join/LumingJoin/src/sequence.txt"
with open(seq_path,"r") as f:
    sequences = json.load(f)

file_name_column_min_max_vals = "/home/sunluming/join/LumingJoin/src/column_max_min.csv"
with open(file_name_column_min_max_vals, 'r') as f:
    data_raw = list(list(rec) for rec in csv.reader(f, delimiter=','))
    column_min_max_vals = {}
    for i, row in enumerate(data_raw[1:]):
        column_min_max_vals[row[0]] = [float(row[1]), float(row[2])]

def normalize_predicates(filter):
    assert filter["left_value"] in column_min_max_vals.keys(), "{} not in keys".format(filter["left_value"])
    min_val = column_min_max_vals[filter["left_value"]][0]
    max_val = column_min_max_vals[filter["left_value"]][1]
    normalized = (float(filter["right_value"]) - min_val) / (max_val - min_val)
    return normalized

def is_number(n):
    try:
        float(n)
    except ValueError:
        return False
    return True

def parse_predicates(condition_filter):
    return_seq = []
    stack = []
    for filter in condition_filter:
        if(filter!=None):
            if(filter["op_type"]=="Bool"):
                stack.append(filter["operator"])
            elif(filter["op_type"]=="Compare"):
                # right value
                right_value = filter["right_value"]
                if("LIKE" in right_value):
                    rv = right_value.split("%")
                    splited_right_value = [(rv[0])]
                    splited_right_value.extend(rv[1].replace("(","").replace(")","").split(" "))
                elif(is_number(right_value)):
                    norm_right_value = normalize_predicates(filter)
                    splited_right_value = norm_right_value
                else:
                    splited_right_value = right_value.split(".")
                stack.append([filter["left_value"].split(".")[0], filter["left_value"].split(".")[1], \
                    filter["operator"], splited_right_value])
            else:
                raise ValueError("Unknown condition filter")
        else:
            return_seq.append(stack.pop())
            return_seq.append(stack.pop())
    if(len(stack)==0):
        pass
    else:
        return_seq.append(stack.pop())
    assert len(stack) == 0
    for each in return_seq:
        pass
    if(return_seq==[]):
        return_seq = [["None"]]
    return return_seq


def node_embedding(node):

    # node_type, sort keys[bag], join condition[bag?], 
    # condition_filter[bag], condition_index[bag?], index_name, est_card, 
    # cost, latency
    node_type = node["node_type"]
    cost = node["cost"]
    latency = node["latency"]
    # print(node_type)
    if(node_type=="Materialize"):
        return [node_type, "None", "None", "None", "None", "None", "None", cost, latency]
    elif(node_type=="Sort"):
        keys = node["sort_keys"]
        bag = []
        for key in keys:
            bag.extend(key.split("."))
        return [node_type, bag, "None", "None", "None", "None","None", cost, latency]
    elif(node_type=="Hash"):
        return [node_type, "None", "None", "None", "None", "None", "None", cost, latency]
    elif(node_type=="Hash Join" or node_type=="Merge Join" or node_type=="Nested Loop"):
        condition = node["condition"]
        join_condition = []
        for each in condition:
            join_condition.extend(each["left_value"].split("."))
            join_condition.extend(each["right_value"].split("."))
        if(join_condition==[]):
            join_condition="None"
        return [node_type, "None", join_condition, "None", "None", "None", "None", cost, latency]
    elif(node_type=="Seq Scan"):
        parsed_predicates = parse_predicates(node["condition_filter"])
        return [node_type, "None", "None", parsed_predicates, "None", "None", node["est_card"], cost, latency]
    elif(node_type=="Bitmap Heap Scan"):
        parsed_predicates = parse_predicates(node["condition_filter"])
        return [node_type, "None", "None", parsed_predicates, "None", "None", node["est_card"], cost, latency]
    elif(node_type=="Index Scan" or node_type=="Bitmap Index Scan"):
        parsed_predicates = parse_predicates(node["condition_filter"])
        parsed_condition_index = parse_predicates(node["condition_index"])
        return [node_type, "None", "None", parsed_predicates, parsed_condition_index, node["index_name"], node["est_card"], cost, latency]
    else:
        raise Exception('Unsupported Node Type: ' + node['Node Type'])
        return None





# embedding ordered sequence into vectors
def embedding(seq):
    r_vec = []
    for node in seq:
        node_vec = node_embedding(node)
        assert len(node_vec)==9
        r_vec.append(node_vec)
    return r_vec
    

# order_seq = "./ordered_seq.txt"
transformed_seq = []
for sequence in sequences:
    # print(sequence)

    transformed_seq.append(embedding(sequence))

with open("transformed_sequence.json","w") as f:
    json.dump(transformed_seq,f)
