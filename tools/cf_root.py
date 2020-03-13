import json

from typing import Dict

from mysqlio.basicio import OpenConnection


def main():
    with OpenConnection() as con:
        freq: Dict[str, int] = {}

        cur = con.cursor(dictionary=True)
        q = """
        select * from reports r, nasdaq n, stocks_index i
        where n.ticker = i.ticker
            and i.index_name = 'sp5'
            and r.cik = n.cik;
        """
        cur.execute(q)
        data = cur.fetchall()
        print(f'total: {len(data)}')

        for row in data:
            struct = json.loads(row['structure'])

            if 'is' not in struct:
                continue
            nodes = struct['is']['nodes']

            if 'us-gaap:NetIncomeLoss' in nodes:
                continue
            if 'us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic' in nodes:
                continue
            if 'us-gaap:EarningsPerShareBasic' in nodes:
                continue
            # if 'us-gaap:NetCashProvidedByUsedInInvestingActivities' in nodes:
            #     continue

            if len(nodes) == 1:
                continue

            for node in nodes:
                freq.setdefault(node, 0)
                freq[node] += 1

        freq_sorted = sorted(freq.items(), key=lambda x: x[1], reverse=True)

        print(freq_sorted[:10])


if __name__ == '__main__':
    main()
