from operation_node import *
from predicates_operation import *
import re
import json

def change_alias2table(column, alias2table):
    relation_name = column.split('.')[0]
    column_name = column.split('.')[1]
    if relation_name in alias2table:
        return alias2table[relation_name] + '.' + column_name
    else:
        return column

def extract_info_from_node(node, alias2table):
    relation_name, index_name = None, None
    if 'Relation Name' in node:
        relation_name = node['Relation Name']
    if 'Index Name' in node:
        index_name = node['Index Name']

    if node['Node Type'] == 'Materialize':
        return Materialize(node['Total Cost'], node['Actual Total Time']), None

    elif node['Node Type'] == 'Hash':
        return Hash(node['Total Cost'], node['Actual Total Time']), None

    elif node['Node Type'] == 'Sort':
        keys = [change_alias2table(key, alias2table) for key in node['Sort Key']]
        return Sort(keys, node['Total Cost'], node['Actual Total Time']), None

    elif node['Node Type'] == 'BitmapAnd':
        return BitmapCombine('BitmapAnd', node['Total Cost'], node['Actual Total Time']), None

    elif node['Node Type'] == 'BitmapOr':
        return BitmapCombine('BitmapOr', node['Total Cost'], node['Actual Total Time']), None

    elif node['Node Type'] == 'Result':
        return Result(node['Total Cost'], node['Actual Total Time']), None

    elif node['Node Type'] == 'Hash Join':
        return Join('Hash Join', pre2seq(node["Hash Cond"], alias2table, relation_name, index_name), node['Total Cost'], node['Actual Total Time']), None

    elif node['Node Type'] == 'Merge Join':
        return Join('Merge Join', pre2seq(node["Merge Cond"], alias2table, relation_name, index_name), node['Total Cost'], node['Actual Total Time']), None

    elif node['Node Type'] == 'Nested Loop':
        if 'Join Filter' in node:
            condition = pre2seq(node['Join Filter'], alias2table, relation_name, index_name)
        else:
            condition = []
        return Join('Nested Loop', condition, node['Total Cost'], node['Actual Total Time']), None

    elif node['Node Type'] == 'Aggregate':
        if 'Group Key' in node:
            keys = [change_alias2table(key, alias2table) for key in node['Group Key']]
        else:
            keys = []
        return Aggregate(node['Strategy'], keys, node['Total Cost'], node['Actual Total Time']), None

    elif node['Node Type'] == 'Seq Scan':
        if 'Filter' in node:
            condition_seq_filter = pre2seq(node['Filter'], alias2table, relation_name, index_name)
        else:
            condition_seq_filter = []
        condition_seq_index, relation_name, index_name = [], node["Relation Name"], None
        return Scan('Seq Scan', condition_seq_filter, condition_seq_index, relation_name, index_name, node['Total Cost'], node['Actual Total Time'], node['Plan Rows']), None

    elif node['Node Type'] == 'Bitmap Heap Scan':
        if 'Filter' in node:
            condition_seq_filter = pre2seq(node['Filter'], alias2table, relation_name, index_name)
        else:
            condition_seq_filter = []
        condition_seq_index, relation_name, index_name = [], node["Relation Name"], None
        return Scan('Bitmap Heap Scan', condition_seq_filter, condition_seq_index, relation_name, index_name, node['Total Cost'], node['Actual Total Time'], node['Plan Rows']), None

    elif node['Node Type'] == 'Index Scan':
        if 'Filter' in node:
            condition_seq_filter = pre2seq(node['Filter'], alias2table, relation_name, index_name)
        else:
            condition_seq_filter = []
        if 'Index Cond' in node:
            condition_seq_index = pre2seq(node['Index Cond'], alias2table, relation_name, index_name)
        else:
            condition_seq_index = []
        relation_name, index_name = node["Relation Name"], node['Index Name']
        if len(condition_seq_index) == 1 and re.match(r'[a-zA-Z]+', condition_seq_index[0].right_value) is not None:
            return Scan('Index Scan', condition_seq_filter, condition_seq_index, relation_name,
                        index_name, node['Total Cost'], node['Actual Total Time']), condition_seq_index
        else:
            return Scan('Index Scan', condition_seq_filter, condition_seq_index, relation_name, index_name, node['Total Cost'], node['Actual Total Time'], node['Plan Rows']), None

    elif node['Node Type'] == 'Bitmap Index Scan':
        if 'Index Cond' in node:
            condition_seq_index = pre2seq(node['Index Cond'], alias2table, relation_name, index_name)
        else:
            condition_seq_index = []
        condition_seq_filter, relation_name, index_name = [], None, node['Index Name']
        if len(condition_seq_index) == 1 and re.match(r'[a-zA-Z]+', condition_seq_index[0].right_value) is not None:
            return Scan('Bitmap Index Scan', condition_seq_filter, condition_seq_index, relation_name,
                        index_name, node['Total Cost'], node['Actual Total Time'], node['Plan Rows']), condition_seq_index
        else:
            return Scan('Bitmap Index Scan', condition_seq_filter, condition_seq_index, relation_name, index_name, node['Total Cost'], node['Actual Total Time'], node['Plan Rows']), None

    elif node['Node Type'] == 'Index Only Scan':
        if 'Index Cond' in node:
            condition_seq_index = pre2seq(node['Index Cond'], alias2table, relation_name, index_name)
        else:
            condition_seq_index = []
        condition_seq_filter, relation_name, index_name = [], None, node['Index Name']
        if len(condition_seq_index) == 1 and re.match(r'[a-zA-Z]+', condition_seq_index[0].right_value) is not None:
            return Scan('Index Only Scan', condition_seq_filter, condition_seq_index, relation_name,
                        index_name, node['Total Cost'], node['Actual Total Time'], node['Plan Rows']), condition_seq_index
        else:
            return Scan('Index Only Scan', condition_seq_filter, condition_seq_index, relation_name, index_name, node['Total Cost'], node['Actual Total Time'], node['Plan Rows']), None

    else:
        raise Exception('Unsupported Node Type: ' + node['Node Type'])
        return None, None


def plan2seq(root, alias2table):
    sequence = []
    join_conditions = []
    node, join_condition = extract_info_from_node(root, alias2table)
    if join_condition is not None:
        join_conditions += join_condition
    sequence.append(node)
    if 'Plans' in root:
        for plan in root['Plans']:
            next_sequence, next_join_conditions = plan2seq(plan, alias2table)
            sequence += next_sequence
            join_conditions += next_join_conditions
    sequence.append("NONE")
    return sequence, join_conditions

def get_plan(root):
    return (root, root['Actual Total Time'], root['Actual Rows'])

class PlanInSeq(object):
    def __init__(self, seq, cost, cardinality):
        self.seq = seq
        self.cost = cost
        self.cardinality = cardinality

def get_alias2table(root, alias2table):
    if 'Relation Name' in root and 'Alias' in root:
        alias2table[root['Alias']] = root['Relation Name']
    if 'Plans' in root:
        for child in root['Plans']:
            get_alias2table(child, alias2table)


def class2json(instance):
    if instance is None:
        return json.dumps({})
    else:
        return json.dumps(todict(instance))


def todict(obj, classkey=None):
    if isinstance(obj, dict):
        data = {}
        for (k, v) in obj.items():
            data[k] = todict(v, classkey)
        return data
    elif hasattr(obj, "_ast"):
        return todict(obj._ast())
    elif hasattr(obj, "__iter__") and not isinstance(obj, str):
        return [todict(v, classkey) for v in obj]
    elif hasattr(obj, "__dict__"):
        data = dict([(key, todict(value, classkey))
                     for key, value in obj.__dict__.items()
                     if not callable(value) and not key.startswith('_')])
        if classkey is not None and hasattr(obj, "__class__"):
            data[classkey] = obj.__class__.__name__
        return data
    else:
        return obj

def feature_extractor(input_path, out_path):
    with open(out_path, 'w') as out:
        with open(input_path, 'r') as f:
            for index, plan in enumerate(f.readlines()):
                if plan != 'null\n':
                    plan = json.loads(plan)[0]['Plan']
                    if plan['Node Type'] == 'Aggregate':
                        plan = plan['Plans'][0]
                    alias2table = {}
                    get_alias2table(plan, alias2table)
                    subplan, cost, cardinality = get_plan(plan)
                    seq, _ = plan2seq(subplan, alias2table)
                    seqs = PlanInSeq(seq, cost, cardinality)
                    out.write(class2json(seqs)+'\n')

def reverse(seq):
    nodes = seq["seq"]
    cost = seq["cost"]
    cardinaltiy = seq["cardinality"]
    return_seq = []
    stack = []
    for node in nodes:
        if(node!="NONE"):
            stack.append(node)
        else:
            return_seq.append(stack.pop())
    assert len(stack) == 0
    return return_seq

# for test
# null = ""
# seq = {"seq": [{"node_type": "Nested Loop", "condition": [], "cost": 325087.79, "latency": 12981.386}, {"node_type": "Hash Join", "condition": [{"op_type": "Compare", "operator": "=", "left_value": "movie_info_idx.movie_id", "right_value": "title.id"}], "cost": 148502.62, "latency": 2764.895}, {"node_type": "Seq Scan", "condition_filter": [{"op_type": "Compare", "operator": ">", "left_value": "movie_info_idx.info", "right_value": "7.0"}], "condition_index": [], "relation_name": "movie_info_idx", "index_name": null, "cost": 25185.44, "latency": 570.288, "est_card": 195345}, "NONE", {"node_type": "Hash", "cost": 73922.68, "latency": 1590.506}, {"node_type": "Seq Scan", "condition_filter": [{"op_type": "Bool", "operator": "AND"}, {"op_type": "Compare", "operator": ">=", "left_value": "title.production_year", "right_value": "2000.000000"}, null, {"op_type": "Compare", "operator": "<=", "left_value": "title.production_year", "right_value": "2010.000000"}], "condition_index": [], "relation_name": "title", "index_name": null, "cost": 73922.68, "latency": 1198.249, "est_card": 1053443}, "NONE", "NONE", "NONE", {"node_type": "Index Scan", "condition_filter": [{"op_type": "Compare", "operator": "=", "left_value": "movie_info.info", "right_value": "__ANY__{Germany,German,USA,American}"}], "condition_index": [{"op_type": "Compare", "operator": "=", "left_value": "movie_info.movie_id", "right_value": "title.id"}], "relation_name": "movie_info", "index_name": "movie_id_movie_info", "cost": 2.15, "latency": 0.123, "est_card": null}, "NONE", "NONE"], "cost": 12981.386, "cardinality": 41884}
# for each in (reverse(seq)):
#     print(each["node_type"])

input_path = "/data/sunluming/datasets/test_files_open_source/plans.json"
tmp_path = "./tmp.txt"
output_path = "./sequence.txt"

feature_extractor(input_path,tmp_path)
with open(tmp_path, "r") as f:
    seqs = f.readlines()

output = []
for index, seq in enumerate(seqs):
    print(index)
    seq = json.loads(seq)
    r_seq = reverse(seq)
    output.append(r_seq)

with open(output_path,"w") as f:
    json.dump(output,f,indent=2)