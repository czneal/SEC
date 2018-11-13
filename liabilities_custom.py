# -*- coding: utf-8 -*-
"""
Created on Sun Apr  1 15:04:37 2018

@author: Asus
"""
import database_operations as do
import classificators as cl
import tree_operations as to
import json
import pandas as pd
import numpy as np
from settings import Settings

class DifferentLiabilities(object):
    def __init__(self):
        self.cldict = {}
        self.cldict["lcpc_old"] = cl.LiabClassPC("LbClf/liabilities_class_dict_v2018-08-17.csv",
                               "LbClf/liabilities_class_pch_v2018-09-17.h5")
        self.cldict["lcpc_m_old"] = cl.LiabClassMixed("LbClf/liabilities_class_dict_v2018-08-17.csv",
                               "LbClf/liabilities_class_pch_v2018-09-17.h5")
        self.cldict["lcpc_new"] = cl.LiabClassPC("LbClf/liabilities_class_dict_v2018-08-17.csv",
                               "LbClf/liabilities_class_pch_v2018-11-10.h5")
        self.cldict["lcpc_m_new"] = cl.LiabClassMixed("LbClf/liabilities_class_dict_v2018-08-17.csv",
                               "LbClf/liabilities_class_pch_v2018-11-10.h5")


    def calc_report_liabilities(self, con, structure, adsh, log):
        structure = json.loads(structure)

        #fetch all facts for this report
        cur = con.cursor(dictionary=True)
        cur.execute("""select n.adsh, n.tag, n.version, n.fy, coalesce(tv.value, n.value) as value
                        from mgnums n
                        left outer join mgtruevalues tv
                        on tv.adsh = n.adsh
                        	and tv.version = n.version
                            and tv.tag = n.tag
                        where n.adsh=%(adsh)s
                        and uom='usd' and n.version<>'mg'""",
                         {"adsh":adsh})
        #make them searcheable
        nums = {r["version"].lower() + ":" + r["tag"].lower(): float(r["value"]) for r in cur}
        #make for each classifier its own facts list
        cl_facts = {name:{} for name in self.cldict}
        cl_facts_leaf = {name:{} for name in self.cldict}
        liabs = {}
        #find "us-gaap:liabilitiesAndStockHoldersEquity"
        for (_, _, _, root) in to.enumerate_tags_basic(structure,
                               tag="us-gaap:LiabilitiesAndStockHoldersEquity",
                               chapter="bs"):
            #go trough structure and test fact names
            for (p, c, leaf) in to.enumerate_tags_parent_child_leaf(root):
                if c == "us-gaap:Liabilities": continue
                if c.lower() not in nums: continue

                for cl_name, cl_obj in self.cldict.items():
                    result = ""
                    predict = cl_obj.predict(p, c)
                    if predict>0.8:
                        cl_facts[cl_name][c] = nums[c.lower()]
                        if leaf:
                            cl_facts_leaf[cl_name][c] = nums[c.lower()]
                        result = "ok"
                    else:
                        result = "fail"
                    log.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}".format(adsh, cl_name, p, c, result, predict))

            #calculate liabilities
            for index, cl_name in enumerate(self.cldict):
                liabs[cl_name + "_leaf"] = to.TreeSum.by_leafs(cl_facts_leaf[cl_name], root)
                liabs[cl_name + "_tops"] = to.TreeSum.by_tops(cl_facts[cl_name], root)
                if len(cl_facts_leaf[cl_name]) > 0:
                    liabs[cl_name + "_sum"] = sum(cl_facts_leaf[cl_name].values())
                else:
                    liabs[cl_name + "_sum"] = None

            add_tags = ["us-gaap:Liabilities", "us-gaap:Assets",
                          "us-gaap:LiabilitiesAndStockHoldersEquity",
                          "us-gaap:StockholdersEquity"]
            for tag in add_tags:
                if tag.lower() in nums:
                    liabs[tag] = nums[tag.lower()]


            break
        return liabs

    def calc_liabilities(self, fy, log):
        try:
            con = do.OpenConnection()
            cur = con.cursor(dictionary=True, buffered=True)
            cur.execute("select * from reports where fin_year=%(fy)s " +
                        Settings.select_limit(),
                        {"fy":fy})

            data = []
            for report_index, report in enumerate(cur):
                liabs = self.calc_report_liabilities(con, report["structure"], report["adsh"], log)
                liabs["adsh"] = report["adsh"]
                liabs["cik"] = report["cik"]
                data.append(liabs)
                print("\rProcessed with {0}".format(report_index+1), end="")

            print()

        finally:
            con.close()

        return pd.DataFrame(data)

class LSHEAdvanced(object):
    def __init__(self, adshs):
        self.count = 0
        self.structures = do.read_report_structures(adshs)
        self.liab = cl.LiabilitiesStaticClassifier("LbClf/liab_stat.csv")
        self.lshe = cl.LSHEDirectChildrenClassifier("lbClf/lshe_direct.csv")

    def calc(self, adsh):
        print("\rprocessed:{0}".format(self.count), end="")
        self.count += 1
        structure = json.loads(self.structures.loc[adsh]["structure"])
        for elem in to._enumerate_tags_basic(structure,
                tag="us-gaap:LiabilitiesAndStockHoldersEquity",
                chapter = "bs"):
            if elem[3] is None or elem[3]["children"] is None:
                return np.nan
            tags = set(t for t in elem[3]["children"])

            nums = do.read_report_nums(adsh)

            val = np.nan
            for t in tags:
                if self.lshe.predict("", t) < 0.8:
                    return np.nan

                if self.liab.predict("", t) > 0.8 and t in nums.index:
                    if np.isnan(val):
                        val = 0.0
                    val += nums.loc[t]["value"]

            return val

