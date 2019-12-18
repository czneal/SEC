# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
from abc import ABCMeta, abstractmethod
from typing import Union, Set, cast, Type

"""
Collection of calculation procedures

Each class should begin with 'mg_' prefix
Each class should inherit CalcStatic or CalcDynamic

run_it() parameters
nums - in case CaclStatic pandas.Series object with index
       from self._dp. It can be treated as dict object
       values can be numpy.nan
       
       in case CalcDynamic pandas.DataFrame object with columns
       from self._dp, index as year and values as data
       values can by numpy.nan
fy - year for calculation. 
     Doesn't mean for CalcStatic.
     All indexes in nums object >= than fy and reverse sorted

For testing every branch of code use test_inidicators.TestStaticDynamic 
test case
"""

class CalcBaseClass(metaclass=ABCMeta):
    def __init__(self):
        self._dp: Set[str] = set()
    
    def dp(self) -> Set[str]:
        return self._dp.copy()
    
    @abstractmethod
    def run_it(self, nums, fy: int) -> Union[float, Type[np.nan]]:
        pass

class CalcStatic(CalcBaseClass):
    @staticmethod
    def btype():
        return('static')
        
class CalcDynamic(CalcBaseClass):
    @staticmethod
    def btype():
        return('dynamic')
    
class mg_income(CalcStatic):
    def __init__(self):
        self._dp = {"us-gaap:NetIncomeLossAvailableToCommonStockholdersDiluted",
                   "us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic",
                   "us-gaap:NetIncomeLoss",
                   "us-gaap:ProfitLoss",
                   "mg_capitalized_software"}    
        
    def run_it(self, nums, fy):
        result = np.nan
        if pd.isna(nums["us-gaap:NetIncomeLossAvailableToCommonStockholdersDiluted"]):
            result = nums["us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic"]
        elif pd.isna(nums["us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic"]):
            result = nums["us-gaap:NetIncomeLossAvailableToCommonStockholdersDiluted"]
        else:
            result = min(nums["us-gaap:NetIncomeLossAvailableToCommonStockholdersDiluted"], 
                         nums["us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic"])
        if pd.isna(result):
            result = nums["us-gaap:NetIncomeLoss"]
        if pd.isna(result):
            result = nums["us-gaap:ProfitLoss"]
        if not pd.isna(nums["mg_capitalized_software"]):
            if pd.isna(result):
                result = - nums["mg_capitalized_software"]
            else:
                result -= nums["mg_capitalized_software"]
        
        if not np.isnan(result):
            return float(result)
        else:
            return result
        
class mg_acquired_realestate(CalcStatic):
    def __init__(self):
        self._dp = {'us-gaap:PaymentsToAcquireRealEstate', 
                    'us-gaap:PaymentsToDevelopRealEstateAssets', 
                    'us-gaap:PaymentsToAcquireRealEstateAndRealEstateJointVentures', 
                    'us-gaap:PaymentsToAcquireRealEstateHeldForInvestment'}

    def run_it(self, params, fy):
        result = params.sum()
        if pd.notna(params).sum() == 0:
            result = np.nan

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_capitalized_software(CalcDynamic):
    def __init__(self):
        self._dp = {'us-gaap:CapitalizedComputerSoftwareNet'}

    def run_it(self, params, fy):
        result = np.nan
        f = params[params['fy'].isin([fy, fy-1])]
        if f.shape[0]>=2:
            result = (f.iloc[0]['us-gaap:CapitalizedComputerSoftwareNet'] - 
                      f.iloc[1]['us-gaap:CapitalizedComputerSoftwareNet'])

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_cash_operating_activities(CalcStatic):
    def __init__(self):
        self._dp = {'us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations', 
                    'us-gaap:NetCashProvidedByUsedInOperatingActivities'}

    def run_it(self, params, fy):
        result = np.nan
        if pd.notna(params["us-gaap:NetCashProvidedByUsedInOperatingActivities"]):
            result = params["us-gaap:NetCashProvidedByUsedInOperatingActivities"]
        else:
            result = params["us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"]

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_equity(CalcDynamic):
    def __init__(self):
        self._dp = {'us-gaap:Assets', 'us-gaap:Liabilities'}

    def run_it(self, params, fy):
        result = np.nan
        f = params[params['fy'] == (fy-1)]
        if f.shape[0]>=1:
            result = (f.iloc[0]['us-gaap:Assets'] - 
                      f.iloc[0]['us-gaap:Liabilities'])
        

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_intangibles(CalcStatic):
    def __init__(self):
        self._dp = {'us-gaap:OtherIntangibleAssetsNet', 
                    'us-gaap:Goodwill', 
                    'us-gaap:IntangibleAssetsNetIncludingGoodwill'}

    def run_it(self, params, fy):
        result = params["us-gaap:IntangibleAssetsNetIncludingGoodwill"]
        ds = [params["us-gaap:Goodwill"], 
              params["us-gaap:OtherIntangibleAssetsNet"]]
        if pd.isna(result):
            result = np.nansum(ds)
            if pd.notna(ds).sum() == 0:
                result = np.nan

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_interest_net(CalcStatic):
    def __init__(self):
        self._dp = {'us-gaap:InterestExpense', 
                    'us-gaap:InterestIncome', 
                    'us-gaap:InterestIncomeExpenseNet'}

    def run_it(self, params, fy):
        result = params["us-gaap:InterestIncomeExpenseNet"]
        if pd.isna(result):
            result = (params["us-gaap:InterestIncome"] - 
                      params["us-gaap:InterestExpense"])                

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_invest_fix_assets(CalcStatic):
    def __init__(self):
        self._dp = {'mg_payments_capital', 
                    'us-gaap:PaymentsToDevelopSoftware', 
                    'us-gaap:PaymentsForSoftware', 
                    'us-gaap:PaymentsToAcquireBusinessesAndInterestInAffiliates', 
                    'us-gaap:PaymentsToAcquireBusinessesGross', 
                    'us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired', 
                    'us-gaap:PaymentsToAcquireSoftware'}

    def run_it(self, params, fy):
        result = params.sum()
        
        if (pd.notna(params["us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired"]) and 
            pd.notna(params["us-gaap:PaymentsToAcquireBusinessesGross"])):
            result -= max(params["us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired"], 
                          params["us-gaap:PaymentsToAcquireBusinessesGross"])
        
        if pd.notna(params).sum() == 0:
            result = np.nan

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_payments_capital(CalcStatic):
    def __init__(self):
        self._dp = {'us-gaap:PaymentsForCapitalImprovements', 
                    'us-gaap:PaymentsToAcquireBuildings', 
                    'us-gaap:PaymentsToAcquirePropertyPlantAndEquipment', 
                    'us-gaap:RepaymentsOfLongTermCapitalLeaseObligations', 
                    'us-gaap:PaymentsToAcquireIntangibleAssets', 
                    'us-gaap:PaymentsToAcquireProductiveAssets', 
                    'us-gaap:PaymentsToAcquireOtherProductiveAssets', 
                    'us-gaap:PaymentsToAcquireEquipmentOnLease'}

    def run_it(self, params, fy):
        result = params.sum()
        if pd.notna(params).sum() == 0:
            result = np.nan

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_provision_for_losses(CalcStatic):
    def __init__(self):
        self._dp = {'us-gaap:ProvisionForLoanAndLeaseLosses', 
                    'us-gaap:ProvisionForLoanLeaseAndOtherLosses'}

    def run_it(self, params, fy):
        result = params["us-gaap:ProvisionForLoanAndLeaseLosses"]
        if pd.isna(result):
            result = params["us-gaap:ProvisionForLoanLeaseAndOtherLosses"]
        elif pd.isna(params["us-gaap:ProvisionForLoanLeaseAndOtherLosses"]):
            result = max(result, 
                         params["us-gaap:ProvisionForLoanLeaseAndOtherLosses"])

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_restructuring_nonrecurring(CalcStatic):
    def __init__(self):
        self._dp = {'us-gaap:OtherNonrecurringExpense', 
                    'us-gaap:RestructuringCosts', 
                    'us-gaap:OtherNonrecurringIncomeExpense', 
                    'us-gaap:RestructuringAndRelatedCostIncurredCost', 
                    'us-gaap:RestructuringCharges', 
                    'us-gaap:RestructuringCostsAndAssetImpairmentCharges'}

    def run_it(self, params, fy):
        result = params.max()

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_roe(CalcStatic):
    def __init__(self):
        self._dp = {'mg_equity', 'mg_income'}

    def run_it(self, params, fy):
        result = np.nan
        if params['mg_equity'] != 0.0:
            result = params['mg_income']/params['mg_equity']

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_roe_average(CalcDynamic):
    def __init__(self):
        self._dp = {'mg_roe'}

    def run_it(self, params, fy):
        pw = pd.notna(params['mg_roe']).sum()
        if pw !=0:
            result = np.power(abs((params['mg_roe'] + 1.0).prod()), 
                              1.0/pw) - 1.0
        else:
            result = np.nan

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_roe_variance(CalcDynamic):
    def __init__(self):
        self._dp = {'mg_roe', 'mg_roe_average'}

    def run_it(self, params, fy):
        pw = pd.notna(params['mg_roe']).sum()
        if pw != 0:
            result = np.sqrt((params['mg_roe'] - 
                              params.iloc[0]['mg_roe_average'])**2).sum()/pw
        else:
            result = np.nan

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_r_capitalized_costs_d(CalcDynamic):
    def __init__(self):
        self._dp = {'mg_r_capitalized_costs'}

    def run_it(self, params, fy):
        result = np.nan
        f = params[params['fy'].isin([fy, fy-1])]
        if f.shape[0]>=2:
            result = f.iloc[0]['mg_r_capitalized_costs'] - f.iloc[1]['mg_r_capitalized_costs']

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_r_cash_buybacks_yld(CalcDynamic):
    def __init__(self):
        self._dp = {'mg_r_cash_buybacks'}

    def run_it(self, params, fy):
        result = -params['mg_r_cash_buybacks'].sum()
        if pd.notna(params['mg_r_cash_buybacks']).sum() == 0:
            result = np.nan

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_r_debt_tangible_assets(CalcStatic):
    def __init__(self):
        self._dp = {'us-gaap:Assets', 
                    'mg_r_capitalized_costs', 
                    'mg_r_intangibles', 
                    'mg_r_liabilities'}

    def run_it(self, params, fy):
        ns = [params['us-gaap:Assets'], -params['mg_r_intangibles'], -params['mg_r_capitalized_costs']]
        if pd.notna(ns).sum() != 0 and np.nansum(ns) != 0.0:
            result = params['mg_r_liabilities']/np.nansum(ns)
        else:
            result = np.nan

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_r_dividents_yld(CalcDynamic):
    def __init__(self):
        self._dp = {'mg_r_cash_dividends_common'}

    def run_it(self, params, fy):
        result = -params['mg_r_cash_dividends_common'].sum()
        if pd.notna(params['mg_r_cash_dividends_common']).sum() == 0:
            result = np.nan

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_r_equity(CalcDynamic):
    def __init__(self):
        self._dp = {'us-gaap:Assets', 'mg_r_liabilities'}

    def run_it(self, params, fy):
        result = np.nan
        f = params[params['fy'] == (fy-1)]
        if f.shape[0]>=1:
            result = f.iloc[0]['us-gaap:Assets'] - f.iloc[0]['mg_r_liabilities']

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_r_free_cashflow(CalcStatic):
    def __init__(self):
        self._dp = {'mg_r_cash_payments_fixed_assets', 
                    'mg_r_cash_payments_other_capital', 
                    'mg_r_cash_acquired_businesses', 
                    'mg_r_interest_expenses', 
                    'us-gaap:mg_tax_rate', 
                    'mg_r_cash_operating_activities'}

    def run_it(self, params, fy):
        result = params['mg_r_interest_expenses']*(1.0 - params['us-gaap:mg_tax_rate'])
        ds = [params['mg_r_cash_operating_activities'],
        result,
        -params['mg_r_cash_payments_fixed_assets'],
        -params['mg_r_cash_payments_other_capital'],
        -params['mg_r_cash_acquired_businesses']]
        result = np.nansum(ds)
        if pd.notna(ds).sum() == 0:
            result = np.nan

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_r_free_cashflow_yld(CalcDynamic):
    def __init__(self):
        self._dp = {'mg_r_free_cashflow'}

    def run_it(self, params, fy):
        result = params['mg_r_free_cashflow'].sum()
        if pd.notna(params['mg_r_free_cashflow']).sum() ==0:
            result = np.nan

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_r_income(CalcStatic):
    def __init__(self):
        self._dp = {'us-gaap:ProfitLoss', 
                    'us-gaap:NetIncomeLoss', 
                    'mg_r_capitalized_costs_d', 
                    'mg_r_income_noncontroling', 
                    'mg_r_dividend_preferred'}

    def run_it(self, params, fy):
        result = params["us-gaap:NetIncomeLoss"]
        if pd.isna(result):
            result = params["us-gaap:ProfitLoss"]
        if pd.notna(result):
            result = np.nansum([result, -params["mg_r_capitalized_costs_d"], -params["mg_r_income_noncontroling"], -params["mg_r_income_noncontroling"], -params["mg_r_dividend_preferred"]])

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_r_income_corrected(CalcStatic):
    def __init__(self):
        self._dp = {'mg_r_income', 
                    'mg_r_one_time_events', 
                    'mg_r_income_equity_method', 
                    'mg_r_unrealized_gain_loss'}

    def run_it(self, params, fy):
        result = -np.nansum([params['mg_r_one_time_events'], params['mg_r_income_equity_method'],
        params['mg_r_unrealized_gain_loss'], -params['mg_r_income']])

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_r_income_corrected_yld(CalcDynamic):
    def __init__(self):
        self._dp = {'mg_r_income_corrected'}

    def run_it(self, params, fy):
        result = params['mg_r_income_corrected'].sum()
        if pd.notna(params['mg_r_income_corrected']).sum() == 0:
            result = np.nan

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_r_income_free_cashflow(CalcStatic):
    def __init__(self):
        self._dp = {'mg_r_income_corrected_yld', 'mg_r_free_cashflow_yld'}

    def run_it(self, params, fy):
        result = np.nan
        if params['mg_r_free_cashflow_yld'] != 0.0:
            result = (params['mg_r_income_corrected_yld']/
                      params['mg_r_free_cashflow_yld'])

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_r_roe(CalcStatic):
    def __init__(self):
        self._dp = {'mg_r_income', 'mg_r_equity'}

    def run_it(self, params, fy):
        result = np.nan
        if params['mg_r_equity'] != 0.0:
            result = params['mg_r_income']/params['mg_r_equity']

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_r_roe_average(CalcDynamic):
    def __init__(self):
        self._dp = {'mg_r_roe'}

    def run_it(self, params, fy):
        pw = pd.notna(params['mg_r_roe']).sum()
        if pw != 0:
            result = np.power(abs((params['mg_r_roe'] + 1.0).prod()), 1.0/pw) -1.0
        else:
            result = np.nan

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_r_roe_variance(CalcDynamic):
    def __init__(self):
        self._dp = {'mg_r_roe', 'mg_r_roe_average'}

    def run_it(self, params, fy):
        pw = pd.notna(params['mg_r_roe']).sum()
        if pw != 0:
            result = np.sqrt((params['mg_r_roe'] - params.iloc[0]['mg_r_roe_average'])**2).sum()/pw
        else:
            result = np.nan

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_r_sales_growth(CalcDynamic):
    def __init__(self):
        self._dp = {'mg_r_sales'}

    def run_it(self, params, fy):
        result = np.nan
        f = params[params['fy'].isin([fy, fy-1])]
        if f.shape[0]>=2 and f.iloc[1]['mg_r_sales'] != 0.0:
            result = float(f.iloc[0]['mg_r_sales'] - f.iloc[1]['mg_r_sales'])/f.iloc[1]['mg_r_sales']

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_r_sales_growth_average(CalcDynamic):
    def __init__(self):
        self._dp = {'mg_r_sales_growth'}

    def run_it(self, params, fy):
        pw = pd.notna(params['mg_r_sales_growth']).sum()
        if pw != 0:
            result = np.power(abs((params['mg_r_sales_growth'] + 1.0).prod()), 1.0/pw) -1.0
        else:
            result = np.nan

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_sales(CalcStatic):
    def __init__(self):
        self._dp = {'us-gaap:Revenues', 'us-gaap:SalesRevenueNet', 'us-gaap:SalesRevenueGoodsNet'}

    def run_it(self, params, fy):
        result = params["us-gaap:Revenues"]
        if pd.isna(result):
            result = params["us-gaap:SalesRevenueNet"]
        if pd.isna(result):
            result = params["us-gaap:SalesRevenueGoodsNet"]

        if not np.isnan(result):
            return float(result)
        else:
            return result

class mg_SGAAExpense(CalcStatic):
    def __init__(self):
        self._dp = {'us-gaap:SellingGeneralAndAdministrativeExpense', 'us-gaap:SellingAndMarketingExpense', 'us-gaap:GeneralAndAdministrativeExpense'}

    def run_it(self, params, fy):
        result = params["us-gaap:SellingGeneralAndAdministrativeExpense"]
        if pd.isna(result):
            ds = [params["us-gaap:SellingAndMarketingExpense"], params["us-gaap:GeneralAndAdministrativeExpense"]]
            result = np.nansum(ds)
            if pd.notna(ds).sum() == 0:
                result = np.nan

        if not np.isnan(result):
            return float(result)
        else:
            return result

if __name__ == '__main__':
    print(issubclass(mg_income, CalcStatic))
    print(mg_income.btype())

