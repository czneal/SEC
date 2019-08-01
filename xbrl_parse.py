import utils
import mysqlio.basicio
from xbrlxml.dataminer import NumericDataMiner
from xbrlxml.xbrlrss import CustomEnumerator
from mysqlio.xbrlfileio import ReportToDB

       
def read(record_enum, repeat, slice_, log_dir, append_log = False):
    records = filesenum.filing_records()
    miner = NumericDataMiner(log_dir=log_dir,
                             repeat_filename=repeat,
                             append_log=append_log)
    mysql_writer = ReportToDB()
    
    pb = utils.ProgressBar()
    pb.start(len(records))    
    
    with mysqlio.basicio.OpenConnection() as con:
        cur = con.cursor(dictionary=True)
        for record, filename in records[slice_]:
            miner.feed(record, filename)            
            mysql_writer.write(cur, record, miner)
            con.commit()
            
            pb.measure()
            print('\r' + pb.message(), end='')
        mysql_writer.flush(cur)
        con.commit()
        
    print()    
    miner.finish()
    
if __name__ == '__main__':        
#    files = XbrlFiles(xbrl = '../test/gen-20121231.xml',
#                      xsd = '../test/gen-20121231.xsd', 
#                      pres = '../test/gen-20121231_pre.xml', 
#                      defi = '../test/gen-20121231_def.xml', 
#                      calc = '../test/gen-20121231_cal.xml')
    filesenum = CustomEnumerator('outputs/customrss.csv')
    read(filesenum, 'outputs/repeatrss.csv', slice(0, 30), 'outputs/')
    
    