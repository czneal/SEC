import pandas as pd
import json

from typing import List, Optional, Tuple, Dict

import xbrlxml.selectors as sel
import mysqlio.basicio as do

from xbrlxml.xbrlchapter import CalcChapter, DimChapter, Node
from xbrlxml.xbrlfile import XbrlFile
from xbrlxml.xbrlrss import CustomEnumerator
from xbrlxml.xbrlzip import XBRLZipPacket
from algos.scheme import enum
from algos.xbrljson import custom_decoder

def check_structure(structure: dict, nums: pd.DataFrame,
                    tres: float=0.02) -> List[str]:
    tree = pd.DataFrame(data=[e for e in enum(structure, outpattern='pcwo')],
                        columns=list('pcwo'))
    tree = tree.merge(nums, how='left', left_on='c', right_on='name')
    tree['v*w'] = tree['value']*tree['w']
    parent_tags = list(tree['p'].unique())
    group = tree.groupby('p')['v*w'].sum()
    
    data = []
    for tag in parent_tags:
        try:
            tag_value = tree[tree['c'] == tag].iloc[0]['value']
            if pd.isna(tag_value):
                continue
        except IndexError:
            continue
        
        try:
            group_value = group[tag]
        except IndexError:
            group_value = None
            
        if group_value is None:
            data.append([tag, tag_value, None, None])
            
        diff = abs(group_value - tag_value)
        mean = (abs(group_value) + abs(tag_value))/2
        if ( diff > 0 and diff/mean > tres):
            data.append([tag, tag_value, group_value, diff])
            
    return data

def check_tag_value(tag: Node,
                    context: str, 
                    dfacts: pd.DataFrame) -> bool:        
    try:
        tag_value = dfacts[(dfacts['name'] == tag.name) &
                          (dfacts['context'] == context)].iloc[0]['value']
    except IndexError:
        return True
    
    if not tag.children:
        return True
    
    sum_tag_value = 0
    values = dfacts[dfacts['context'] == context]
    children = pd.DataFrame(data=[[c.name, c.getweight()] 
                                    for c in tag.children.values()],
                            columns=['name', 'weight'])
    values = pd.merge(values, children,
                      how='inner',
                      left_on='name', right_on='name')
    values = values.astype({'value': 'float', 'weight': 'float'})
    if values.shape[0] > 0:
        sum_tag_value = (values['weight']*values['value']).sum()
    else:
        sum_tag_value = None
    
    return tag_value == sum_tag_value

def calc_missing_value(name: str, 
               context: str, 
               dfacts: pd.DataFrame,
               contexts,
               pres: DimChapter) -> float:
    
    chapter_dims = set([(d, m) for [d, m] in pres.dimmembers()])
    context_dims = set([(d, m) for d, m in 
                              zip(contexts[context].dim,
                                  contexts[context].member)])
    dims = chapter_dims.difference(context_dims)
    dims = pd.DataFrame(dims, columns=['d', 'm'])
    
    df = dfacts[dfacts['name'] == name]
    fact_contexts = list(df['context'].unique())
    
    cntx = []
    for c in contexts.values():
        if ((c.contextid in fact_contexts) and
            (c.axises() == contexts[context].axises() + 1)):
            cntx.extend(c.asdictdim())
    cntx = pd.DataFrame(cntx)
    
    cntx = pd.merge(df, cntx,
                    left_on='context', right_on='context',
                    suffixes=('','_y'))
    cntx = pd.merge(cntx, dims,
                    left_on=['dim', 'member'], right_on=['d', 'm'])
        
            
    return cntx.groupby(['dim', 'edate', 'uom'])['value'].sum().reset_index()

def find_missing_values(tag: Node,
                        context: str, 
                        dfacts: pd.DataFrame) -> List[str]:
    tags = set([c.name for c in tag.children.values()])
    f = dfacts[(dfacts['context'] == context) &
               (dfacts['name'].isin(tags))]
    fact_tags = f['name'].unique()
    
    return list(tags.difference(fact_tags))

def repair_value(adsh: str, value_name: str, sheet: str) -> float:
    from utils import add_root_dir
    
    with do.OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        cur.execute('select cik, adsh, period, file_date, ' + 
                    'file_link, contexts, structure '+
                    'from reports where adsh=%s', (adsh,))
        record = cur.fetchall()[0]
    
    record['company_name'] = 'fake'
    zip_filename = add_root_dir(record['file_link'])
    contexts = json.loads(record['contexts'])
    structure = json.loads(record['structure'])
    
    packet = XBRLZipPacket()
    packet.open_packet(zip_filename)
    
    xbrlfile = XbrlFile()
    xbrlfile.prepare(packet, record)    
    xbrlfile.read_units_facts_fn()
    
    roleuri = structure[sheet]['chapter']['roleuri']
    for e in contexts:
        if e['roleuri'] == roleuri:
            context = e['context']
            break
        
    calc = xbrlfile.schemes['calc'][roleuri]
    pres = xbrlfile.schemes['pres'][roleuri]
    
    cntx = calc_missing_value(name=value_name,
                              context=context,
                              contexts=xbrlfile.contexts,
                              dfacts=xbrlfile.dfacts,
                              pres=pres)
                
    if cntx.shape[0] == 1:
        return cntx.iloc[0]['value']
    else:
        return 0
    
def open_file(adsh: str) -> Tuple[XbrlFile, Dict[str, str]]:
    from utils import add_root_dir
    
    with do.OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        cur.execute('select cik, adsh, period, file_date, ' + 
                    'file_link, contexts, structure '+
                    'from reports where adsh=%s', (adsh,))
        record = cur.fetchall()[0]
    
    record['company_name'] = 'fake'
    zip_filename = add_root_dir(record['file_link'])
    contexts = json.loads(record['contexts'])
    structure = json.loads(record['structure'])
    
    packet = XBRLZipPacket()
    packet.open_packet(zip_filename)
    
    xbrlfile = XbrlFile()
    xbrlfile.prepare(packet, record)    
    xbrlfile.read_units_facts_fn()
    
    cntx = {}
    for sheet in structure:
        roleuri = structure[sheet]['chapter']['roleuri']
        for e in contexts:
            if e['roleuri'] == roleuri:
                cntx[sheet] = e['context']
                break
    return xbrlfile, cntx
    
def make_tree(adsh: str, structure: Optional[dict]) -> pd.DataFrame:
    if structure is None:
        structures = do.read_report_structures([adsh])
        structure = json.loads(structures.loc[adsh]['structure'],
                                   object_hook=custom_decoder)
        
    nums = do.read_reports_nums([adsh])
    nums['name'] = nums['version'] + ':' + nums['tag']
    
    
    frames = []
    for sheet in structure:
        data = [e for e in enum(structure[sheet]['chapter'],
                                outpattern='pcow')]
        frame = pd.DataFrame(data, columns=list('pcow'))
        frame['sheet'] = sheet
        frames.append(frame)
    
    df = pd.concat(frames)
    df = pd.merge(df, nums[['name', 'value']], how='left',
                  left_on='c', right_on='name')
    df['v*w'] = df['value']*df['w']
    df['c'] = df.apply(lambda x: '' + 
                                    '__'.join(['' for e in range(x['o'] + 1)]) +
                                    '|' + x['c'],
                             axis=1)
    
    return df[['sheet', 'c', 'value', 'w', 'v*w', 'p']]

def make_tree_from(structure, nums: pd.DataFrame) -> pd.DataFrame:
    frames = []
    for sheet in structure:
        data = [e for e in enum(structure[sheet]['chapter'],
                                outpattern='pcow')]
        frame = pd.DataFrame(data, columns=list('pcow'))
        frame['sheet'] = sheet
        frames.append(frame)
    
    df = pd.concat(frames)
    df = pd.merge(df, nums[['name', 'value']], how='left',
                  left_on='c', right_on='name')
    df['v*w'] = df['value']*df['w']
    df['c'] = df.apply(lambda x: '' + 
                                    '__'.join(['' for e in range(x['o'] + 1)]) +
                                    '|' + x['c'],
                             axis=1)
    return df[['sheet', 'c', 'value', 'w', 'v*w', 'p']]
    
def misscalculated(adshs: List[str]) -> pd.DataFrame:
    structures = do.read_report_structures(adshs)
    nums = do.read_reports_nums(adshs)
    nums['name'] = nums['version'] + ':' + nums['tag']
    nums['context'] = 'context'
    
    data = []
    for adsh in adshs:
        structure = json.loads(structures.loc[adsh]['structure'],
                               object_hook=custom_decoder)
        dfacts = nums[nums['adsh']==adsh]
        for sheet in structure:
            chapter = structure[sheet]['chapter']
            misscalc = check_structure(chapter, 
                                       dfacts)
            for [tag, tag_value, group_value, diff] in misscalc:
                missing = find_missing_values(chapter.getnode(tag),
                                              context='context',
                                              dfacts=dfacts)
                if missing:
                    data.extend([[adsh, sheet, 
                                  tag, tag_value, group_value, diff,
                                  m] for m in missing])
                else:
                    data.append([adsh, sheet,
                                 tag, tag_value, group_value, diff,
                                 None])
    
    df = pd.DataFrame(data, columns=['adsh', 'sheet', 
                                     'misscalc', 
                                     'tag_value', 'group_value', 'diff',
                                     'missing'])
    return df

def calc_structure(structure: dict, facts: Dict[str, float],
                   none_sum_err: bool) -> list:
    def calc_one_fact(node: Node, facts: Dict[str, float],
                      none_sum_err: bool,
                      log: list) -> Optional[float]:
        value = facts.get(node.name, None)
        
        s = []
        for child in node.children.values():
            v = calc_one_fact(child, facts, none_sum_err, log)
            if v is not None:
                s.append(v*child.getweight())
        if s:
            value_sum = sum(s)
        elif not node.children:
            value_sum = value
        else:
            value_sum = None
            
        if value is None:
            if value_sum is None:
                return None
            else:
                facts[node.name] = value_sum
                return value_sum
        else:            
            if value_sum != value:
                if ((value_sum is not None) or
                    (none_sum_err and value_sum is None)):
                    log.append([node.name, value, value_sum])
                                
        return value
    
    log = []
    for sheet in structure:
        chapter = structure[sheet]['chapter']
        for child in chapter.nodes.values():
            if child.parent is not None:
                continue
            calc_one_fact(child, facts, none_sum_err, log)
            
    return log

if __name__ == '__main__':
#    with do.OpenConnection() as con:
#        cur = con.cursor(dictionary=True)
#        cur.execute("""select adsh, r.cik from reports r, nasdaq n
#                        where n.cik = r.cik
#                             and file_date>'2019-01-01'
#                        group by adsh, r.cik
#                        order by market_cap desc
#                        limit 100""")
#        adsh_cik = pd.DataFrame(cur.fetchall())
#        adshs = list(adsh_cik['adsh'].unique())
    
    adshs=['0000034088-19-000010']
    df = misscalculated(adshs)
    tree = make_tree(adshs[0], None)
    
#    xbrlfile, cntx = open_file(adshs[0])
#    df = xbrlfile.dfacts
#    df = df[df['context'] == cntx['bs']]
#    
#    import structure.taxonomy as tx
#    tax = tx.Taxonomy(gaap_id='2018-01-31')
#    tax.read()
#    
#    structure = json.loads(tax.taxonomy.iloc[5]['structure'],
#                           object_hook=custom_decoder)
#    structure = {'bs': {'chapter': structure,
#                        'label': 'balance sheet'}}
#    
#    nums = do.read_reports_nums(adshs)
#    nums['name'] = nums['version'] + ':' + nums['tag']
##    xbrlfile, cntx = open_file(adshs[0])
##    nums = xbrlfile.dfacts[xbrlfile.dfacts['context'] == cntx['bs']]
#    nums = nums.dropna(subset=['value'])
#    facts = {}
#    for index, row in nums.iterrows():
#        facts[row['name']] = float(row['value'])
#        
#    log = calc_structure(structure, facts, none_sum_err=False)
##    
#    tree = make_tree_from(structure, nums)
#    df = df.merge(adsh_cik, left_on='adsh', right_on='adsh')
#    tree = make_tree(adshs[0])
#    
#    val = repair_value(adshs[0], 
#                       'us-gaap:ContractWithCustomerLiabilityCurrent',
#                       'bs')
    
    
#    
#    tree = make_tree(adshs[0], structure)
    


    
    