import time
from multiprocessing import Pool
from mysql.connector.errors import InternalError
import mysqlio.basicio
from xbrlxml.xbrlexceptions import XbrlException

def write(p: int) -> int:
    sic = [1600, 1623, 1700]
    
    insert = """insert into companies (cik,isin,company_name,sic) values({0},NULL,'test{0}',{1}) on duplicate key update isin=values(isin),company_name=values(company_name),sic=values(sic)""".format(p, sic[p-1])
    
    count = 0
    sec_sleep = 3
    with mysqlio.basicio.OpenConnection() as con:
        while(True):
            try:
                cur = con.cursor(dictionary=True)
                cur.execute(insert)
                time.sleep(sec_sleep)
                con.commit()
                break
            except InternalError:
                sec_sleep = 0
                count += 1
                continue
        
    return p, count

if __name__ == "__main__":
#    cpus = 3
#    
#    params = [[1], [2], [3]]
#
#    with Pool(cpus) as p:
#        print(p.starmap(write, params))
    while(True):
        try:
            raise XbrlException('message')        
        except XbrlException:
            continue
        except Exception:
            break
        finally:            
            print('finally')
            break
        
