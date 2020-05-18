# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 14:05:20 2019

@author: Asus
"""

import re
import lxml
import lxml.etree  # type: ignore
from collections import namedtuple

XSDChapter = namedtuple('XSDChapter',
                        ['roleuri', 'label', 'sect', 'id'])


class XSDFile():
    def read(self, xsd_file):
        chapters = {}
        '''
        doc - document entry - matchDocument
        sta - financial statements - matchStatement
        not - notes to financial statements - all others
        pol - accounting policies - matchPolicy
        tab - notes tables - matchTable
        det - notes details - matchDetail
        '''
        matchStatement = re.compile(r'.* +\- +Statement +\- .*')
        matchDisclosure = re.compile(r'.* +\- +Disclosure +\- +.*')
        matchDocument = re.compile(r'.* +\- +Document +\- +.*')
        matchParenthetical = re.compile(r'.*\-.+-.*Paren.+')
        matchPolicy = re.compile(r'.*\(.*Polic.*\).*')
        matchTable = re.compile(r'.*\(Table.*\).*')
        matchDetail = re.compile(r'.*\(Detail.*\).*')

        root = lxml.etree.parse(xsd_file)

        for role in root.iter('{*}roleType'):
            rol_def = role.find("{*}definition")

            sect = ""
            if matchDocument.match(rol_def.text):
                sect = "doc"
            elif matchStatement.match(rol_def.text):
                if matchParenthetical.match(rol_def.text):
                    sect = "sta"
                else:
                    sect = "sta"
            elif matchDisclosure.match(rol_def.text):
                if matchPolicy.match(rol_def.text):
                    sect = "pol"
                elif matchTable.match(rol_def.text):
                    sect = "tab"
                elif matchDetail.match(rol_def.text):
                    sect = "det"
                else:
                    sect = "not"
            roleuri = role.attrib["roleURI"]

            label = re.sub(r'\d+\s*-\s*\w*\s*-\s*', '',
                           rol_def.text.strip(), flags=re.I)
            chapters[roleuri] = XSDChapter(roleuri, label, sect,
                                           role.attrib["id"])
        return chapters


def main():
    xsd = XSDFile()
    xsd.read('../test/aal-20181231.xsd')


if __name__ == '__main__':
    main()
