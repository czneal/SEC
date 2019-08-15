import os
from multiprocessing import Pool

import utils
import mysqlio.basicio
from xbrlxml.dataminer import NumericDataMiner
from xbrlxml.xbrlrss import CustomEnumerator
from mysqlio.xbrlfileio import ReportToDB
from log_file import Logs, RepeatFile

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
    logs = Logs(log_dir, append_log=append_log)
    repeat = RepeatFile(repeat)
    
    miner = NumericDataMiner(logs=logs,
                             repeat=repeat)
    mysql_writer = ReportToDB(logs=logs,
                              repeat=repeat)
    
    pb = utils.ProgressBar()
    pb.start(len(records))    
    
    with mysqlio.basicio.OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        for record, filename in records[slice_]:
            logs.set_header([record['cik'], record['adsh'], filename])
            repeat.set_state(record, filename)
            
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
    
def multiproc_read():
    cpus = os.cpu_count() - 1
    print('run {0} proceses'.format(cpus))
    
    params = []
    filesenum = CustomEnumerator('outputs/customrss.csv')
    records = filesenum.filing_records()
    records_per_cpu = int(len(records)/cpus) + 1
    for i, start in enumerate(range(0, len(records), records_per_cpu)):
        args = [records,
                'outputs/multiproc/repeat{0}.csv'.format(i),
                slice(start, start + records_per_cpu),
                'outputs/multiproc/logs{0}/'.format(i)]
        params.append(args)
        
        if not os.path.exists(args[3]):
            os.mkdir(args[3])
            
    last_slice = params[-1][2]
    params[-1][2] = slice(last_slice.start, None)
    
    with Pool(cpus) as p:
        print(p.starmap(read, params))
    

if __name__ == '__main__':        
#    files = XbrlFiles(xbrl = '../test/gen-20121231.xml',
#                      xsd = '../test/gen-20121231.xsd', 
#                      pres = '../test/gen-20121231_pre.xml', 
#                      defi = '../test/gen-20121231_def.xml', 
#                      calc = '../test/gen-20121231_cal.xml')
    filesenum = CustomEnumerator('outputs/testrss.csv')
    read(filesenum.filing_records(), 
         'outputs/repeatrss.csv', 
         slice(0, None), 
         'outputs/')
    
#    multiproc_read()
#    concat_logs_repeat(logs_dir='z:/Projects/SEC/SEC/Outputs/multiproc/', 
#                       output_dir='outputs/') 
    