import numpy as np
import pandas as pd
null = None
fy = 2016

#mg_acquired_realestate, static, 0
params = {"us-gaap:PaymentsToDevelopRealEstateAssets":null,
"us-gaap:PaymentsToAcquireRealEstateHeldForInvestment":null,
"us-gaap:PaymentsToAcquireRealEstateAndRealEstateJointVentures":null,
"us-gaap:PaymentsToAcquireRealEstate":null}

result = params.sum()
if pd.notna(params).sum() == 0:
     result = np.nan


#mg_capitalized_software, dynamic, 0
params = {"us-gaap:CapitalizedComputerSoftwareNet":2}

result = np.nan
f = params[params['fy'].isin([fy, fy-1])]
if f.shape[0]>=2:
    result = f.iloc[0]['us-gaap:CapitalizedComputerSoftwareNet'] - f.iloc[1]['us-gaap:CapitalizedComputerSoftwareNet']


#mg_cash_operating_activities, static, 0
params = {"us-gaap:NetCashProvidedByUsedInOperatingActivities":null,
"us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations":null}

result = np.nan
if pd.notna(params["us-gaap:NetCashProvidedByUsedInOperatingActivities"]):
    result = params["us-gaap:NetCashProvidedByUsedInOperatingActivities"]
else:
    result = params["us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"]


#mg_equity, dynamic, 0
params = {"us-gaap:Assets":2, "us-gaap:Liabilities":2}

result = np.nan
f = params[params['fy'] == (fy-1)]
if f.shape[0]>=1:
    result = f.iloc[0]['us-gaap:Assets'] - f.iloc[0]['us-gaap:Liabilities']



#mg_income, static, 0
params = {"us-gaap:NetIncomeLossAvailableToCommonStockholdersDiluted":null,
"us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic":null,
"us-gaap:NetIncomeLoss":null,
"us-gaap:ProfitLoss":null,
"mg_capitalized_software":null}

result = np.nan
if pd.isna(params["us-gaap:NetIncomeLossAvailableToCommonStockholdersDiluted"]):
    result = params["us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic"]
elif pd.isna(params["us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic"]):
    result = params["us-gaap:NetIncomeLossAvailableToCommonStockholdersDiluted"]
else:
    result = min(params["us-gaap:NetIncomeLossAvailableToCommonStockholdersDiluted"], params["us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic"])
if pd.isna(result):
    result = params["us-gaap:NetIncomeLoss"]
if pd.isna(result):
    result = params["us-gaap:ProfitLoss"]
if not pd.isna(params["mg_capitalized_software"]):
    if pd.isna(result):
        result = - params["mg_capitalized_software"]
    else:
        result -= params["mg_capitalized_software"]


#mg_intangibles, static, 0
params = {
	"us-gaap:Goodwill" : null,
	"us-gaap:IntangibleAssetsNetIncludingGoodwill" : null,
	"us-gaap:OtherIntangibleAssetsNet" : null
}

result = params["us-gaap:IntangibleAssetsNetIncludingGoodwill"]
ds = [params["us-gaap:Goodwill"], params["us-gaap:OtherIntangibleAssetsNet"]]
if pd.isna(result):
    result = np.nansum(ds)
    if pd.notna(ds).sum() == 0:
        result = np.nan


#mg_interest_net, static, 0
params = {"us-gaap:InterestIncomeExpenseNet":null,
"us-gaap:InterestIncome":null,
"us-gaap:InterestExpense":null}

result = params["us-gaap:InterestIncomeExpenseNet"]
if pd.isna(result):
    result = params["us-gaap:InterestIncome"] - params["us-gaap:InterestExpense"]
        


#mg_invest_fix_assets, static, 0
params = {"us-gaap:PaymentsToAcquireBusinessesGross":null,
"us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired":null,
"us-gaap:PaymentsToAcquireBusinessesAndInterestInAffiliates":null,
"us-gaap:PaymentsToAcquireSoftware":null,
"us-gaap:PaymentsToDevelopSoftware":null,
"us-gaap:PaymentsForSoftware":null,
"mg_payments_capital":null}

result = params.sum()

if pd.notna(params["us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired"]) and pd.notna(params["us-gaap:PaymentsToAcquireBusinessesGross"]):
    result -= max(params["us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired"], params["us-gaap:PaymentsToAcquireBusinessesGross"])

if pd.notna(params).sum() == 0:
    result = np.nan


#mg_payments_capital, static, 0
params = {"us-gaap:PaymentsForCapitalImprovements":null,
"us-gaap:PaymentsToAcquireBuildings":null,
"us-gaap:PaymentsToAcquirePropertyPlantAndEquipment":null,
"us-gaap:PaymentsToAcquireOtherProductiveAssets":null,
"us-gaap:PaymentsToAcquireProductiveAssets":null,
"us-gaap:PaymentsToAcquireIntangibleAssets":null,
"us-gaap:RepaymentsOfLongTermCapitalLeaseObligations":null,
"us-gaap:PaymentsToAcquireEquipmentOnLease":null}

result = params.sum()
if pd.notna(params).sum() == 0:
     result = np.nan


#mg_provision_for_losses, static, 0
params = {"us-gaap:ProvisionForLoanAndLeaseLosses":null,
"us-gaap:ProvisionForLoanLeaseAndOtherLosses":null}

result = params["us-gaap:ProvisionForLoanAndLeaseLosses"]
if pd.isna(result):
    result = params["us-gaap:ProvisionForLoanLeaseAndOtherLosses"]
elif pd.isna(params["us-gaap:ProvisionForLoanLeaseAndOtherLosses"]):
    result = max(result, params["us-gaap:ProvisionForLoanLeaseAndOtherLosses"])


#mg_restructuring_nonrecurring, static, 0
params = {"us-gaap:OtherNonrecurringExpense":null,
"us-gaap:RestructuringCharges":null,
"us-gaap:RestructuringCosts":null,
"us-gaap:RestructuringAndRelatedCostIncurredCost":null,
"us-gaap:OtherNonrecurringIncomeExpense":null,
"us-gaap:RestructuringCostsAndAssetImpairmentCharges":null}

result = params.max()


#mg_roe, static, 0
params = {"mg_income":null, "mg_equity":null}

result = np.nan
if params['mg_equity'] != 0.0:
     result = params['mg_income']/params['mg_equity']


#mg_roe_average, dynamic, 1
params = {"mg_roe":-1}

pw = pd.notna(params['mg_roe']).sum()
if pw !=0:
     result = np.power(abs((params['mg_roe'] + 1.0).prod()), 1.0/pw) -1.0
else:
     result = np.nan


#mg_roe_variance, dynamic, 1
params = {"mg_roe":-1, "mg_roe_average":-1}

pw = pd.notna(params['mg_roe']).sum()
if pw != 0:
     result = np.sqrt((params['mg_roe'] - params.iloc[0]['mg_roe_average'])**2).sum()/pw
else:
     result = np.nan


#mg_r_capitalized_costs_d, dynamic, 0
params = {"mg_r_capitalized_costs":2}

result = np.nan
f = params[params['fy'].isin([fy, fy-1])]
if f.shape[0]>=2:
    result = f.iloc[0]['mg_r_capitalized_costs'] - f.iloc[1]['mg_r_capitalized_costs']


#mg_r_cash_buybacks_yld, dynamic, 1
params = {"mg_r_cash_buybacks":-1}

result = params['mg_r_cash_buybacks'].sum()
if pd.notna(params['mg_r_cash_buybacks']).sum() == 0:
     result = np.nan


#mg_r_debt_tangible_assets, static, 1
params = {"us-gaap:Assets":null, "mg_r_intangibles":null, "mg_r_capitalized_costs":null, "mg_r_liabilities":null}

ns = [params['us-gaap:Assets'], -params['mg_r_intangibles'], -params['mg_r_capitalized_costs']]
if pd.notna(ns).sum() != 0 and np.nansum(ns) != 0.0:
     result = params['mg_r_liabilities']/np.nansum(ns)
else:
     result = np.nan


#mg_r_dividents_yld, dynamic, 1
params = {"mg_r_cash_dividends_common":-1}

result = params['mg_r_cash_dividends_common'].sum()
if pd.notna(params['mg_r_cash_dividends_common']).sum() == 0:
     result = np.nan


#mg_r_equity, dynamic, 0
params = {"us-gaap:Assets":2, "mg_r_liabilities":2}

result = np.nan
f = params[params['fy'] == (fy-1)]
if f.shape[0]>=1:
    result = f.iloc[0]['us-gaap:Assets'] - f.iloc[0]['mg_r_liabilities']


#mg_r_free_cashflow, static, 0
params = {"mg_r_cash_operating_activities":null, 
"mg_r_interest_expenses":null, 
"mg_r_cash_payments_fixed_assets":null, 
"mg_r_cash_payments_other_capital":null, 
"mg_r_cash_acquired_businesses":null, 
"us-gaap:mg_tax_rate":null}

result = params['mg_r_interest_expenses']*(1.0 - params['us-gaap:mg_tax_rate'])
ds = [params['mg_r_cash_operating_activities'],
                  result,
                  -params['mg_r_cash_payments_fixed_assets'],
                  -params['mg_r_cash_payments_other_capital'],
                  -params['mg_r_cash_acquired_businesses']]
result = np.nansum(ds)
if pd.notna(ds).sum() == 0:
     result = np.nan


#mg_r_free_cashflow_yld, dynamic, 1
params = {"mg_r_free_cashflow":-1}

result = params['mg_r_free_cashflow'].sum()
if pd.notna(params['mg_r_free_cashflow']).sum() ==0:
     result = np.nan


#mg_r_income, static, 0
params = {"us-gaap:NetIncomeLoss":null, "us-gaap:ProfitLoss":null, "mg_r_capitalized_costs_d":null, "mg_r_income_noncontroling":null, "mg_r_dividend_preferred":null}

result = params["us-gaap:NetIncomeLoss"]
if pd.isna(result):
    result = params["us-gaap:ProfitLoss"]
if pd.notna(result):
    result = np.nansum([result, -params["mg_r_capitalized_costs_d"], -params["mg_r_income_noncontroling"], -params["mg_r_income_noncontroling"], -params["mg_r_dividend_preferred"]])


#mg_r_income_corrected, static, 0
params = {"mg_r_income":null, "mg_r_one_time_events":null, "mg_r_income_equity_method":null, "mg_r_unrealized_gain_loss":null}

result = -np.nansum([params['mg_r_one_time_events'], params['mg_r_income_equity_method'],
                    params['mg_r_unrealized_gain_loss'], -params['mg_r_income']])


#mg_r_income_corrected_yld, dynamic, 1
params = {"mg_r_income_corrected":-1}

result = params['mg_r_income_corrected'].sum()
if pd.notna(params['mg_r_income_corrected']).sum() == 0:
     result = np.nan


#mg_r_income_free_cashflow, static, 1
params = {"mg_r_income_corrected_yld":null, "mg_r_free_cashflow_yld":null}

result = np.nan
if params['mg_r_free_cashflow_yld'] != 0.0:
     result = params['mg_r_income_corrected_yld']/params['mg_r_free_cashflow_yld']


#mg_r_roe, static, 0
params = {"mg_r_income":null, "mg_r_equity":null}

result = np.nan
if params['mg_r_equity'] != 0.0:
     result = params['mg_r_income']/params['mg_r_equity']


#mg_r_roe_average, dynamic, 1
params = {"mg_r_roe":-1}

pw = pd.notna(params['mg_r_roe']).sum()
if pw != 0:
     result = np.power(abs((params['mg_r_roe'] + 1.0).prod()), 1.0/pw) -1.0
else:
     result = np.nan


#mg_r_roe_variance, dynamic, 1
params = {"mg_r_roe":-1, "mg_r_roe_average":-1}

pw = pd.notna(params['mg_r_roe']).sum()
if pw != 0:
     result = np.sqrt((params['mg_r_roe'] - params.iloc[0]['mg_r_roe_average'])**2).sum()/pw
else:
     result = np.nan


#mg_r_sales_growth, dynamic, 0
params = {"mg_r_sales":2}

result = np.nan
f = params[params['fy'].isin([fy, fy-1])]
if f.shape[0]>=2 and f.iloc[1]['mg_r_sales'] != 0.0:
    result = float(f.iloc[0]['mg_r_sales'] - f.iloc[1]['mg_r_sales'])/f.iloc[1]['mg_r_sales']


#mg_r_sales_growth_average, dynamic, 1
params = {"mg_r_sales_growth":-1}

pw = pd.notna(params['mg_r_sales_growth']).sum()
if pw != 0:
     result = np.power(abs((params['mg_r_sales_growth'] + 1.0).prod()), 1.0/pw) -1.0
else:
     result = np.nan


#mg_sales, static, 0
params = {"us-gaap:Revenues":null,
"us-gaap:SalesRevenueNet":null,
"us-gaap:SalesRevenueGoodsNet":null}

result = params["us-gaap:Revenues"]
if pd.isna(result):
    result = params["us-gaap:SalesRevenueNet"]
if pd.isna(result):
    result = params["us-gaap:SalesRevenueGoodsNet"]


#mg_SGAAExpense, static, 0
params = {"us-gaap:SellingGeneralAndAdministrativeExpense":null,
"us-gaap:GeneralAndAdministrativeExpense":null,
"us-gaap:SellingAndMarketingExpense":null}

result = params["us-gaap:SellingGeneralAndAdministrativeExpense"]
if pd.isna(result):
    ds = [params["us-gaap:SellingAndMarketingExpense"], params["us-gaap:GeneralAndAdministrativeExpense"]]
    result = np.nansum(ds)
    if pd.notna(ds).sum() == 0:
         result = np.nan


