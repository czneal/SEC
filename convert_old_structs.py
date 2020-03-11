import json
from typing import Dict, Iterator, Tuple

from algos.scheme import enum
from algos.xbrljson import loads, dumps
from mysqlio.readers import MySQLReader
from mysqlio.writers import MySQLWriter
from utils import ProgressBar


class MySQLIter(MySQLReader):
    def fetch_reports(self) -> Iterator[Tuple
                                        [str, Dict[str, Dict[str, dict]]]]:
        try:
            self.cur.execute(
                "select adsh, structure from reports;")
            while True:
                row = self.cur.fetchone()
                if not row:
                    break

                structure = json.loads(row['structure'])

                new_struct = {}
                for sheet, chapter in structure.items():
                    if 'chapter' not in chapter:
                        continue

                    new_struct[sheet] = chapter['chapter']
                    new_struct[sheet]['label'] = chapter['label']

                if new_struct == {}:
                    continue

                yield (row['adsh'], new_struct)
        except Exception:
            return


class StrucWriter(MySQLWriter):
    def write(self, obj: Tuple[str, str]):
        try:
            self.cur.execute(
                'update reports set structure=%(structure)s where adsh = %(adsh)s', {
                    'adsh': obj[0], 'structure': obj[1]})
        except Exception:
            return


def convert_old_structures():
    it = MySQLIter()
    w = StrucWriter()
    pb = ProgressBar()

    pb.start()
    for adsh, structure in it.fetch_reports():
        s_str = dumps(loads(json.dumps(structure)))
        w.write((adsh, s_str))
        w.flush()
        pb.measure()
        print('\r' + pb.message(), end='')
    print()

    it.close()
    w.close()


if __name__ == '__main__':
    convert_old_structures()
