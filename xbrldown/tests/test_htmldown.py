import unittest

from xbrldown.htmldown import get_html_report_link


class TestHtmlDownload(unittest.TestCase):
    def test_find_report_link(self):
        with self.subTest(i=0):
            index_link = "https://www.sec.gov/Archives/edgar/data/7332/0000007332-16-000038-index.htm"

            link = get_html_report_link(index_link)
            self.assertEqual(
                link,
                'https://sec.gov/Archives/edgar/data/7332/000000733216000038/swn20151231x10k.htm'
            )

        with self.subTest(i=1):
            index_link = 'https://www.sec.gov/Archives/edgar/data/815097/0000815097-16-000030-index.htm'
            link = get_html_report_link(index_link)
            self.assertEqual(
                link,
                'https://sec.gov/Archives/edgar/data/815097/000081509716000030/a2015form10-kfrontpart.htm'
            )

        with self.subTest(i=1):
            index_link = 'https://www.sec.gov/Archives/edgar/data/764764/0000764764-20-000032-index.htm'
            link = get_html_report_link(index_link)
            self.assertEqual(
                link,
                'https://sec.gov/Archives/edgar/data/764764/000076476420000032/cfsc-12312019x10k.htm'
            )


if __name__ == '__main__':
    unittest.main()
