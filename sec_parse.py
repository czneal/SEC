import os
import pandas as pd
from multiprocessing import Pool
from typing import List

import mysqlio.basicio
from indi.types import TRecordPair
from xbrlxml.dataminer import NumericDataMiner
from xbrlxml.xbrlrss import CustomEnumerator
from mysqlio.xbrlfileio import ReportToDB
from log_file import Logs, RepeatFile
from settings import Settings
from utils import ProgressBar, remove_root_dir

def concat_records(r1: List[TRecordPair],
                   r2: List[TRecordPair]) -> List[TRecordPair]:
    index = set()
    new_list : List[TRecordPair] = []
    for r, filename in r1 + r2:
        if filename in index:
            continue
        index.add(filename)
        new_list.append([r, filename])
    
    return new_list
                   
def concat_files(filenames, output):
    with open(output, 'w') as f:
        for filename in filenames:
            with open(filename, 'r') as source:
                f.write(source.read())
                
def concat_logs_repeat(logs_dir, output_dir):
    repeat = []
    logs = []
    errs = []
    warns = []
    for (root, dirs, filenames) in os.walk(logs_dir):
        for filename in filenames:
            if filename.endswith('.err'):
                errs.append(root + '/' + filename)
            if filename.endswith('.log'):
                logs.append(root + '/' + filename)
            if filename.endswith('.warn'):
                warns.append(root + '/' + filename)
            if filename.startswith('repeat'):
                repeat.append(root + '/' + filename)
    
    concat_files(repeat, output_dir + 'repeat.rss')
    concat_files(logs, output_dir + 'log.log')
    concat_files(warns, output_dir + 'log.warn')
    concat_files(errs, output_dir + 'log.err')
            
    
def read(records, repeat, slice_, log_dir, append_log=False):
    logs = Logs(log_dir, append_log=append_log, name='parse_log')
    repeat = RepeatFile(repeat)
    
    miner = NumericDataMiner(logs=logs,
                             repeat=repeat)
    mysql_writer = ReportToDB(logs=logs,
                              repeat=repeat)
    
    pb = ProgressBar()
    pb.start(len(records[slice_]))
        
    with mysqlio.basicio.OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        for record, filename in records[slice_]:
            logs.set_header([record['cik'], record['adsh'], 
                             remove_root_dir(filename)])
            repeat.set_state(record, remove_root_dir(filename))
            
            if miner.feed(record, filename):
                mysql_writer.write(cur, record, miner)
                con.commit()
            
            pb.measure()
            print('\r' + pb.message(), end='')
        mysql_writer.flush(cur)
        con.commit()
        
    print()
    logs.close()    
    repeat.close()
    
    return(pb.n)
    
def multiproc_read(rss_filename):
    cpus = os.cpu_count() - 1
    print('run {0} proceses'.format(cpus))
    
    params = []
    filename = os.path.join(Settings.app_dir(), 
                            Settings.output_dir(), 
                            rss_filename)
    filesenum = CustomEnumerator(filename)
    records = filesenum.filing_records()
    
    multiproc_dir = os.path.join(Settings.app_dir(), 
                           Settings.output_dir(),
                           'multiproc/')
                           
    if not os.path.exists(multiproc_dir):
        os.mkdir(multiproc_dir)
        
    records_per_cpu = int(len(records)/cpus) + 1
    for i, start in enumerate(range(0, len(records), records_per_cpu)):
        args = [records,
                os.path.join(multiproc_dir, 'repeat{0}.csv'.format(i)),
                slice(start, start + records_per_cpu),
                os.path.join(multiproc_dir, 'logs{0}/'.format(i))
               ]
        params.append(args)
        
        if not os.path.exists(args[3]):
            os.mkdir(args[3])
            
    last_slice = params[-1][2]
    params[-1][2] = slice(last_slice.start, None)
    
    with Pool(cpus) as p:
        print(p.starmap(read, params))
        
def make_cond_rss(query: str, 
                  rss_to: str,
                  rss_from: str) -> None:
    """
    only for internal use
    query must contain columns adsh, file_link
    """
    from utils import remove_common_path
    
    with mysqlio.basicio.OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        cur.execute(query)
        files = pd.DataFrame(cur.fetchall())
        if files.shape[0] == 0:
            return
    
    files['file_link'] = files['file_link'].apply(
            lambda x: remove_common_path('d:/sec', x))
    df = pd.read_csv(rss_from, sep='\t', names=['record', 'filename'])
    f = df[df['filename'].isin(files['file_link'].unique())]
    f.to_csv(rss_to, sep='\t', header=False, index=False, quotechar="[")
    

if __name__ == '__main__':        
#    files = XbrlFiles(xbrl = '../test/gen-20121231.xml',
#                      xsd = '../test/gen-20121231.xsd', 
#                      pres = '../test/gen-20121231_pre.xml', 
#                      defi = '../test/gen-20121231_def.xml', 
#                      calc = '../test/gen-20121231_cal.xml')
    filesenum = CustomEnumerator('outputs/customrss.csv')
    read(filesenum.filing_records(), 
         'outputs/repeatrss.csv', 
         slice(0, None), 
         'outputs/')
    
#    make_cond_rss("""select adsh, file_link from reports
#                     where structure like '%"roleuri": "roleuri"%'
#                     """,
#                  rss_from='outputs/all.csv',
#                  rss_to='outputs/customrss.csv')
    
#    multiproc_read('all.csv')
#    concat_logs_repeat(logs_dir='/home/victor/sec/outputs/multiproc/', 
#                       output_dir='/home/victor/sec/outputs/') 
    