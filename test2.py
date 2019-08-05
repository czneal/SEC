import json
import pandas as pd

if __name__ == '__main__':
    a = json.loads("""[["54 CONSOLIDATED STATEMENTS OF EQUITY (Cloud Peak Energy Inc. and Subsidiaries)", 38], ["79 CONSOLIDATED BALANCE SHEETS (Parenthetical) (Cloud Peak Energy Inc. and Subsidiaries)", 6], ["80 CONSOLIDATED STATEMENTS OF OPERATIONS AND COMPREHENSIVE INCOME (Cloud Peak Energy Inc. and Subsidiaries)", 38], ["81 CONSOLIDATED BALANCE SHEETS (Cloud Peak Energy Inc. and Subsidiaries)", 45], ["82 CONSOLIDATED STATEMENTS OF CASH FLOWS (Cloud Peak Energy Inc. and Subsidiaries)", 52], ["83 CONSOLIDATED STATEMENTS OF EQUITY (Cloud Peak Energy Resources LLC and Subsidiaries)", 24], ["84 CONSOLIDATED STATEMENTS OF OPERATIONS AND COMPREHENSIVE INCOME (Cloud Peak Energy Resources LLC and Subsidiaries)", 33], ["85 CONSOLIDATED BALANCE SHEETS (Cloud Peak Energy Resources LLC and Subsidiaries)", 45], ["86 CONSOLIDATED STATEMENTS OF CASH FLOWS (Cloud Peak Energy Resources LLC and Subsidiaries)", 53]]
""")
    df = pd.DataFrame(data=a, columns=['name', 'p'])
    df = df.sort_values('p', ascending=False)
    f = df[(df['name'].str.contains('Energy Inc'))]
    
    
    ch = {f.loc[3]['name']: 'bs', 
          f.loc[2]['name']: 'is', 
          f.loc[4]['name']: 'cf'}
    print(json.dumps(ch))