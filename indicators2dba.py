# -*- coding: utf-8 -*-
"""
Created on Wed Apr  3 18:12:42 2019

@author: Asus
"""

import database_operations as do

def write_mg_descr(ind_pool):
    command = """
    insert into mgparamsdesc (pname, description, type)
    values(%(pname)s, %(description)s, %(type)s)
    on duplicate key update
    description = values(description), type = values(type)
    """
    data = []
    for ind_name, ind in ind_pool.indicators.items():
        data.append({'description':ind.description()[1], 'pname':ind_name, 'type':1})

    con = None
    try:
        con = do.OpenConnection()
        cursor = con.cursor()
        cursor.executemany(command, data)
        con.commit()
    except:
        raise
    finally:
        if con: con.close()
    return

def write_gaap_descr(gaap_tags):
    con = None
    try:
        con = do.OpenConnection()
        cursor = con.cursor()
        format_strings = ','.join(['%s'] * len(gaap_tags))
        cmd = """insert into mgparamsdesc
                       select concat(version, ':', tag), docum.text, 1 from docum
                       where concat(version, ':', tag) in ({0})
                           and  type = '{1}'
                       on duplicate key update
                       description = docum.text, type = 1"""
        cursor.execute( cmd.format(format_strings, 'label'), tuple(gaap_tags))
        cursor.execute( cmd.format(format_strings, 'documentation'), tuple(gaap_tags))

        con.commit()
    except:
        raise
    finally:
        if con: con.close()
    return

def write_params(params):
    con = None
    try:
        con = do.OpenConnection()
        cursor = con.cursor(dictionary=True)
        table = do.Table('mgparamstype1', con)
        table.write_df(params, cursor)
        table.flush(cursor)
        con.commit()
    except:
        raise
    finally:
        if con: con.close()
    return

def write_report(nums, cik, adsh):
    con = None
    try:
        con = do.OpenConnection()
        cursor = con.cursor(dictionary=True)
        table = do.Table('mgreporttable', con)
        n = nums[(nums['tag'].isin(table.fields)) & (nums['adsh'] == adsh)]
        n = n.pivot(index='adsh', columns='tag', values='value').reset_index()
        n['cik'] = cik
        table.write_df(n, cursor)
        table.flush(cursor)
        con.commit()
    except:
        raise
    finally:
        if con: con.close()
    return