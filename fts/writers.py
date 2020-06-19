import typing

from mysqlio.writers import MySQLWriter
from mysqlio.basicio import MySQLTable


class HtmlTextWriter(MySQLWriter):
    def __init__(self):
        super().__init__()

        self.html_reports = MySQLTable('html_reports', con=self.con)

    def write(self, obj: typing.Tuple[str, str]):
        """obj must by tuple (adsh: str, text: str)"""

        self.write_to_table(
            self.html_reports, {
                'adsh': obj[0], 'report_text': obj[1]})
