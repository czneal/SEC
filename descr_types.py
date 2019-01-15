# -*- coding: utf-8 -*-
"""
Created on Wed Dec 12 17:45:34 2018

@author: Asus
"""

from xml_tools import XmlTreeTools
import pandas as pd
import database_operations as do
from urltools import fetch_urlfile
import urllib

def read_xsd_elements_xmltree(tree):
    root = tree.root
    data = []
    for elem in root.findall(tree.xsd+"element"):
#        abstract = 0
#        xsd_id = 1
#        name = 2
#        nillable = 3
#        substitutionGroup = 4
#        type = 5
#        xbrli:periodType = 6
        row = [False, None, None, True, None, None, None, None]

        if 'abstract' in elem.attrib:
            row[0] = bool(elem.attrib['abstract'])
        if 'id' in elem.attrib:
            row[1] = elem.attrib['id'].strip()
        if 'name' in elem.attrib:
            row[2] = elem.attrib['name'].strip()
        if 'nillable' in elem.attrib:
            row[3] = bool(elem.attrib['nillable'].strip())
        if 'substitutionGroup' in elem.attrib:
            row[4] = elem.attrib['substitutionGroup'].strip()
        if 'type' in elem.attrib:
            row[5] = elem.attrib['type'].strip()
        if tree.xbrli + 'periodType' in elem.attrib:
            row[6] = elem.attrib[tree.xbrli + 'periodType'].strip()
        if tree.xbrli + 'balance' in elem.attrib:
            row[7] = elem.attrib[tree.xbrli + 'balance'].strip()

        data.append(row)

    df = pd.DataFrame(data, columns=['abstract','xsd_id','tag','nillable',
                                'sgroup', 'type', 'period',
                                'balance'])
    df['version'] = df['xsd_id'].apply(lambda x: x.split('_')[0])

    return df

def read_xsd_elements(xsd_filename):
    if xsd_filename is None:
        return None

    if (type(xsd_filename) == str and
        urllib.parse.urlparse(xsd_filename).scheme.lower() in {'http','https'}):
        xsd_filename = fetch_urlfile(xsd_filename)

    tree = XmlTreeTools()
    tree.read_xml_tree(xsd_filename)

    return read_xsd_elements_xmltree(tree)

def extract_attrib(elem, attrib):
    row = [None for e in attrib]
    for i, a in enumerate(attrib):
        if a in elem.attrib:
            row[i] = elem.attrib[a]
    return row

def read_documentation(xml_filename):
    if xml_filename is None:
        return None

    if (type(xml_filename) == str and
        urllib.parse.urlparse(xml_filename).scheme.lower() in {'http','https'}):
        xml_filename = fetch_urlfile(xml_filename)

    tree = XmlTreeTools()
    tree.read_xml_tree(xml_filename)
    root = tree.root
    loc = []
    lab = []
    loclab = []
    for elem in root.iter():
        if elem.tag.lower() == tree.link+'loc':
            xsd_id = elem.attrib[tree.xlink + 'href'].split('#')[-1].strip()
            xlabel = elem.attrib[tree.xlink + 'label']
            loc.append([xsd_id, xlabel])
        if elem.tag.lower() == tree.link+'label':
            row = extract_attrib(elem, ['id', tree.xlink+'label', tree.xlink+'role',
                                        tree.xml + 'lang'])
            row[2] = row[2].split('/')[-1]
            if elem.text is None:
                row.append('')
            else:
                row.append(elem.text.strip())
            lab.append(row)
        if elem.tag.lower() == tree.link + 'labelarc':
            loclab.append(extract_attrib(elem, [tree.xlink + 'from', tree.xlink + 'to']))

    loc = pd.DataFrame(loc, columns=['xsd_id', 'loc_id'])
    loclab = pd.DataFrame(loclab, columns=['loc_id', 'lab_id'])
    lab = pd.DataFrame(lab, columns=['id', 'lab_id', 'type', 'lang', 'text'])

    loc = loc.merge(loclab, 'inner', left_on='loc_id', right_on='loc_id')
    lab = lab.merge(loc, 'inner', left_on='lab_id', right_on='lab_id')

    lab['version'] = lab['xsd_id'].apply(lambda x: x.split('_')[0])
    lab['tag'] = lab['xsd_id'].apply(lambda x: x.split('_')[-1])

    return lab

def load_gaap_taxonomy(xsd_filename, doc_filename, lab_filename):
    con = None
    try:
        con = do.OpenConnection()
        cur = con.cursor(dictionary=True)

        docum = do.Table('docum', con)
        lab = read_documentation(doc_filename)
        docum.write_df(lab, cur)

        lab = read_documentation(lab_filename)
        docum.write_df(lab, cur)
        con.commit()

        mgtags = do.Table('mgtags', con, buffer_size=1)
        elems = read_xsd_elements(xsd_filename)
        mgtags.write_df(elems, cur)
        con.commit()

    except:
        raise
    finally:
        if con:
            con.close()

#xsd = 'http://xbrl.fasb.org/us-gaap/2016/elts/us-gaap-2016-01-31.xsd'
#
#df = read_xsd_elements('Test/us-gaap-2017-01-31/elts/us-gaap-2017-01-31.xsd')

#lab = read_documentation('Test/acf-20161231_lab.xml')

#load_gaap_taxonomy('Test/us-gaap-2017-01-31/elts/us-gaap-2017-01-31.xsd',
#                   'Test/us-gaap-2017-01-31/elts/us-gaap-doc-2017-01-31.xml',
#                   'Test/us-gaap-2017-01-31/elts/us-gaap-lab-2017-01-31.xml')