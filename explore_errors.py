# -*- coding: utf-8 -*-
"""
Created on Wed Sep 19 18:42:39 2018

@author: Asus
"""

import pandas as pd
import numpy as np
import database_operations as do
from settings import Settings
import tree_operations as to
import json

def read_class_log():
    df = (pd.read_csv("outputs/diffliabs/2018-11-13/liab_class.log",
                names = ["adsh","alg","ptag","ctag","status","val"],
                            sep = "\t")
            .replace(to_replace="\[+|\]+", value="", regex=True)
            .drop_duplicates(["adsh","ptag","ctag","alg"])
            .set_index(["adsh","ptag","ctag","alg"])
        )
    df["val"] = df["val"].astype('float')

    return df.unstack().sort_index()

def read_errors(reports):
    df = (pd.read_csv("outputs/diffliabs/2018-11-13/liab_custom_lshe.csv")
            .set_index("adsh")
            .sort_index()
            .merge(reports, how='inner', left_index=True, right_index=True))
    url = "https://www.sec.gov/cgi-bin/viewer?action=view&cik={0}&accession_number={1}&xbrl_type=v#"
    df["url"] = df.apply(lambda x: url.format(x["cik_x"], x.name), axis=1)
    return df

def make_structure(adsh, structure, nums):
    values = []

    for (_, _, _, liab) in to.enumerate_tags_basic(structure,
                                tag="us-gaap:LiabilitiesAndStockHoldersEquity",
                                chapter="bs"):
        for node in to.enumerate_tags_basic_leaf(liab):
            row=[adsh, node[0], node[1], node[2], node[4], node[5]]
            if node[1] in nums.index:
                row.append(nums.loc[node[1]]["value"])
            else:
                row.append(None)
            values.append(row)

    return values

def find_missed_values(adsh, calc_val, real_val, nums):
    diff = abs(calc_val - real_val)

    miss_vals = []

    for tag, value in nums.iterrows():
        if abs(value["value"]) == diff:
            miss_vals.append([adsh, tag, value["value"]])

    return miss_vals

class ErrorsAndLogs(object):
    def __init__(self):
        self.errors = None
        self.log = None
        self.main_columns = None

        self.read_data()


    def read_data(self):
        print("load errors, classification log, report attributes...",end="")
        years = list(range(Settings.years()[0], Settings.years()[1]+1))
        reports = do.read_reports_attr(years)
        self.errors = read_errors(reports)
        self.log = read_class_log()

        cols = list(self.errors.columns)
        vcols = list(self.errors.filter(like = 'lcpc', axis=1))
        vcols += list(self.errors.filter(like = 'lshe', axis=1))
        for c in vcols:
            cols.remove(c)
        ix1 = cols.index('us-gaap:Liabilities')
        ix2 = cols.index('us-gaap:Assets')
        cols[ix2] = 'us-gaap:Liabilities'
        cols[ix1] = 'us-gaap:Assets'
        self.main_columns = cols

        print("ok")

    def write_to_excel(f, tree, stat, filename):
        print("save results to excel...", end="")
        writer = pd.ExcelWriter(filename, engine='xlsxwriter')
        workbook = writer.book
        # Add some cell formats.
        format1 = workbook.add_format({'num_format': '#,##0.00'})
        format2 = workbook.add_format({'num_format': '0%'})

        f.to_excel(writer, sheet_name="errors")
        stat.to_excel(writer, sheet_name="statistics")
        tree.to_excel(writer, sheet_name="sructure")

        # Set the column width and format.
        worksheet = writer.sheets["errors"]
        worksheet.set_column('C:J', 18, format1)
        worksheet.set_column('E:E', 8, format2)
        worksheet.set_column('K:K', 25, None)
        worksheet.set_column('A:A', 20, None)

        worksheet = writer.sheets["statistics"]
        worksheet.set_column('A:A', 30, None)
        worksheet.set_column('B:B', 18, format1)

        worksheet = writer.sheets["sructure"]
        worksheet.set_column('B:B', 20, None)
        worksheet.set_column('C:D', 30, None)
        worksheet.set_column('G:G', 18, format1)
        worksheet.set_column('H:Q', 10, None)

        writer.save()
        print("ok")
        return

    def filter_errors(self, vcol, assets_limit=10000000):
        print("filter errors...", end="")
        vcol_err = "err_" + vcol
        f = self.errors
        f = f[(f[vcol].notnull()) & (f["us-gaap:Liabilities"].notnull())]
        f = f[f[vcol_err] > 0.1]
        f = f[f["us-gaap:Assets"]>=assets_limit]

        f = f.sort_values(vcol_err, ascending=False)
        print("ok")
        return f

    def set_columns(self, f, vcol):
        return f[self.main_columns[0:2] + [vcol, 'err_'+vcol, 'lshe_adv'] + self.main_columns[2:]]

    def statistic(self, vcol):
        return pd.DataFrame(None, columns=["name", "value"]).set_index("name")

    def explain(self, algs, suff, f, filename_end, assets_limit=10000000):
        if suff == "":
            vcol = algs[0]
        else:
            vcol = algs[0] + "_" + suff

        if f is None:
            f = self.filter_errors(vcol)
        f = self.set_columns(f, vcol)

        stat = self.statistic(vcol)
        tree = self.attach_log(f, algs)

        ErrorsAndLogs.write_to_excel(f, tree, stat,
                        Settings.output_dir() + "liab_{0}_{1}.xlsx".format(vcol,filename_end))

        return

    def attach_log(self, res, algs):
        print("make data structure for {0} reports...".format(res.shape[0]), end="")
        structures = do.read_report_structures(res.index)
        tree = []
        for adsh, row in res.iterrows():
            nums = do.read_report_nums(adsh)
            tree.extend(make_structure(adsh,
                                       json.loads(structures.loc[adsh]["structure"]),
                                       nums))


        tree = (pd.DataFrame(tree, columns=["adsh", "ptag", "ctag", "w", "offset", "leaf", "value"])
                    #.set_index(["adsh", "ptag", "ctag"])
                    #.sort_index()
                    )
        columns = list(tree.columns)

        tree = pd.merge(tree, self.log, how='left',
                        left_on=["adsh", "ptag", "ctag"],
                        right_index=True)
        tree.rename(lambda x: (str(x).
                                       replace("('","").
                                       replace("')","").
                                       replace("', '","_")
                                       ), axis=1, inplace=True
                                     )
        for alg in algs:
            columns.extend(list(tree.filter(like=alg, axis=1).columns))
        tree["ctag"] = tree["offset"] + tree["ctag"]
        columns.remove('offset')
        print("ok")

        return tree[columns]

def filter_err_liab_isnull(df, vcol):
    f = df[df["us-gaap:Liabilities"].isnull()]
    print(f.shape)
    f = f[f["lshe_adv"] == f['lshe_she']]
    print(f.shape)
    f = f[f[vcol] != f["lshe_adv"]]
    f['err'] = np.abs((f[vcol] - f['lshe_adv'])/f['lshe_adv'])
    f.sort_values('err', ascending=False)
    return f

algs = ["lcpc_m_new_sum", "lcpc_m_new", "lcpc_old", "lcpc_m_old", "lcpc_new"]

Err = ErrorsAndLogs()
Err.explain(algs, '', None, '')
Err.explain(algs[1:], 'tops', None, '')

f = filter_err_liab_isnull(Err.errors, algs[1] + '_tops')
Err.explain(algs[1:], 'tops', f, 'na')

f = filter_err_liab_isnull(Err.errors, algs[2] + '_sum')
Err.explain(algs[2:], 'sum', f, 'na')



