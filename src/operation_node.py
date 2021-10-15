class Materialize(object):
    def __init__(self, cost, latency):
        self.node_type = 'Materialize'
        self.cost = cost
        self.latency = latency
        
    def __str__(self):
        return 'Materialize'


class Aggregate(object):
    def __init__(self, strategy, keys, cost, latency):
        self.node_type = 'Aggregate'
        self.strategy = strategy
        self.group_keys = keys
        self.cost = cost
        self.latency = latency
        
    def __str__(self):
        return 'Aggregate ON: ' + ','.join(self.group_keys)


class Sort(object):
    def __init__(self, sort_keys, cost, latency):
        self.sort_keys = sort_keys
        self.node_type = 'Sort'
        self.cost = cost
        self.latency = latency
        
    def __str__(self):
        return 'Sort by: ' + ','.join(self.sort_keys)


class Hash(object):
    def __init__(self, cost, latency):
        self.node_type = 'Hash'
        self.cost = cost
        self.latency = latency
        
    def __str__(self):
        return 'Hash'


class Join(object):
    def __init__(self, node_type, condition_seq, cost, latency):
        self.node_type = node_type
        self.condition = condition_seq
        self.cost = cost
        self.latency = latency
        
    def __str__(self):
        return self.node_type + ' ON ' + ','.join([str(i) for i in self.condition])


class Scan(object):
    def __init__(self, node_type, condition_seq_filter, condition_seq_index, relation_name, index_name, cost, latency, cardinality=None):
        self.node_type = node_type
        self.condition_filter = condition_seq_filter
        self.condition_index = condition_seq_index
        self.relation_name = relation_name
        self.index_name = index_name
        self.cost = cost
        self.latency = latency
        self.est_card = cardinality
        
    def __str__(self):
        return self.node_type + ' ON ' + ','.join([str(i) for i in self.condition_filter]) + '; ' + ','.join(
            [str(i) for i in self.condition_index])


class BitmapCombine(object):
    def __init__(self, operator, cost, latency):
        self.node_type = operator
        self.cost = cost
        self.latency = latency
        
    def __str__(self):
        return self.node_type


class Result(object):
    def __init__(self, cost, latency):
        self.node_type = 'Result'
        self.cost = cost
        self.latency = latency
        
    def __str__(self):
        return 'Result'
