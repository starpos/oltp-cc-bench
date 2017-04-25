import collections


def make_dictlist(fpath: str):
    '''
        Args:
            fpath: str - log file path
        Returns:
            [{str:str}] - list of items. An item per a log record.
    '''
    li = []
    with open(fpath) as f:
        for line in f:
            li.append(dict(item.split(':') for item in line.strip().split()))
    return li


def default_classifier(d):
    cond0 = d['nrMutexPerTh'] == '4000' and d['workload'] == 'shortlong' and d['shortMode'] == '0'
    if cond0 and (d['mode'] in ['nowait', 'leis', 'wait-die', 'silo-occ', 'tictoc', 'trlock', 'trlock-occ']):
        return d['mode']
    if cond0 and (d['mode'] in ['trlock-hybrid']):
        if d['pqLockType'] == '0':
            return 'trlock-hybrid'
        if d['pqLockType'] == '4':
            return 'trlock-hybrid-pqmcs2'
    return None


def default_key(name, d):
    return name, int(d['concurrency'])


def classify(li, key=default_key, classifier=default_classifier):
    '''
        Args:
            li: [{str:str}] - list of items. make_dictlist() returned value.
        Returns:
            {key: value} - key: (name: str, concurrency: int), value: [{str:str}]
    '''
    g = collections.defaultdict(list)
    for d in li:
        name = classifier(d)
        if name is None:
            continue
        k = key(name, d)
        g[k].append(d)
    return g
    

def make_plot_data(g, accumulate):
    '''
        Args:
            g: classify() returned value.
            accumulate: [{str:str}] -> [any printable]
        Returns:
            [any printable]
    '''
    li = []
    for key, dl in g.items():
        li.append(list(key) + accumulate(dl))
    return li


def save_plot_data(li, fpath):
    '''
        Args:
            li: [any printable] - make_plot_data() returned value.
            fpath: str - file path to save.
        Returns:
            None
    '''
    with open(fpath, 'w') as f:
        for item in sorted(li, key=lambda x: (x[0], x[1])):
            print(*item, file=f)
