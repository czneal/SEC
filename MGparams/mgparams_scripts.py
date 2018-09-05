# -*- coding: utf-8 -*-
"""
Created on Fri Jan 12 19:46:05 2018

@author: Asus
"""

#mg_cash_operating_activities
params = """{"NetCashProvidedByUsedInOperatingActivities":null,
"NetCashProvidedByUsedInOperatingActivitiesContinuingOperations":null}"""

result = None
if params["NetCashProvidedByUsedInOperatingActivities"] is not None:
    result = params["NetCashProvidedByUsedInOperatingActivities"]
else:
    result = params["NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"]
    

#mg_intangibles
params = """{"IntangibleAssetsNetIncludingGoodwill":null,
"OtherIntangibleAssetsNet":null, "Goodwill":null}"""

result = None
if params["IntangibleAssetsNetIncludingGoodwill"] is not None:
    result = params["IntangibleAssetsNetIncludingGoodwill"]
else:
    result = 0    
    if params["Goodwill"] is not None:
        result += params["Goodwill"]
    if params["OtherIntangibleAssetsNet"] is not None:
        result += params["OtherIntangibleAssetsNet"]
        
#mg_income
params = """{"NetIncomeLossAvailableToCommonStockholdersDiluted":null,
"NetIncomeLossAvailableToCommonStockholdersBasic":null,
"NetIncomeLoss":null,
"ProfitLoss":null,
"mg_capitalized_software":null}"""
result = None
if params["NetIncomeLossAvailableToCommonStockholdersDiluted"] is None:
    result = params["NetIncomeLossAvailableToCommonStockholdersBasic"]
elif params["NetIncomeLossAvailableToCommonStockholdersBasic"] is None:
    result = params["NetIncomeLossAvailableToCommonStockholdersDiluted"]
else:
    result = min(params["NetIncomeLossAvailableToCommonStockholdersDiluted"], params["NetIncomeLossAvailableToCommonStockholdersBasic"])
if result is None:
    result = params["NetIncomeLoss"]
if result is None:
    result = params["ProfitLoss"]
if params["mg_capitalized_software"] is not None:
    if result is None:
        result = - params["mg_capitalized_software"]
    else:
        result -= params["mg_capitalized_software"]

    
#mg_sales
params="""{"Revenues":null,
"SalesRevenueNet":null,
"SalesRevenueGoodsNet":null}"""
result = params["Revenues"]
if result is None:
    result = params["SalesRevenueNet"]
if result is None:
    result = params["SalesRevenueGoodsNet"]
    
#mg_SGAAExpense
params = """{"SellingGeneralAndAdministrativeExpense":null,
"GeneralAndAdministrativeExpense":null,
"SellingAndMarketingExpense":null}"""
result = params["SellingGeneralAndAdministrativeExpense"]
if result is None:
    result = params["GeneralAndAdministrativeExpense"]
    if params["SellingAndMarketingExpense"] is not None:
        if result is None:
            result = params["SellingAndMarketingExpense"]
        else:
            result += params["SellingAndMarketingExpense"]

#mg_restructuring_nonrecurring
params = """{"OtherNonrecurringExpense":null,
"RestructuringCharges":null,
"RestructuringCosts":null,
"RestructuringAndRelatedCostIncurredCost":null,
"OtherNonrecurringIncomeExpense":null,
"RestructuringCostsAndAssetImpairmentCharges":null}"""
result = params["OtherNonrecurringExpense"]
for k in params:
    if result is None:
        result = params[k]
    elif params[k] is not None:
        if result < params[k]:
            result = params[k]
            
#mg_provision_for_losses
params = """{"ProvisionForLoanAndLeaseLosses":null,
"ProvisionForLoanLeaseAndOtherLosses":null}"""
result = params["ProvisionForLoanAndLeaseLosses"]
if result is None:
    result = params["ProvisionForLoanLeaseAndOtherLosses"]
elif params["ProvisionForLoanLeaseAndOtherLosses"] is not None:
    result = max(result, params["ProvisionForLoanLeaseAndOtherLosses"])
    
#mg_capitalized_software
params = """{"CapitalizedComputerSoftwareNet":[null, null]}"""
result = params["CapitalizedComputerSoftwareNet"][1]
if result is not None and params["CapitalizedComputerSoftwareNet"][0] is not None:
    result = result - params["CapitalizedComputerSoftwareNet"][0]
else:
    result = None
    
#mg_acquired_realestate
params="""{"PaymentsToDevelopRealEstateAssets":null
"PaymentsToAcquireRealEstateHeldForInvestment":null,
"PaymentsToAcquireRealEstateAndRealEstateJointVentures":null,
"PaymentsToAcquireRealEstate":null}"""
result = None
for k in params:
    if params[k] is not None:
        if result is not None:
            result = result + params[k]
        else:
            result = params[k]
            
#mg_payments_capital
params = """{"PaymentsForCapitalImprovements":null,
"PaymentsToAcquireBuildings":null,
"PaymentsToAcquirePropertyPlantAndEquipment":null,
"PaymentsToAcquireOtherProductiveAssets":null,
"PaymentsToAcquireProductiveAssets":null,
"PaymentsToAcquireIntangibleAssets":null,
"RepaymentsOfLongTermCapitalLeaseObligations":null,
"PaymentsToAcquireEquipmentOnLease":null}"""
result = None
for k in params:
    if params[k] is not None:
        if result is not None:
            result = result + params[k]
        else:
            result = params[k]
            
            
#mg_invest_fix_assets
params="""{"PaymentsToAcquireBusinessesGross":null,
"PaymentsToAcquireBusinessesNetOfCashAcquired":null,
"PaymentsToAcquireBusinessesAndInterestInAffiliates":null,
"PaymentsToAcquireSoftware":null,
"PaymentsToDevelopSoftware":null,
"PaymentsForSoftware":null,
"mg_payments_capital":null}"""
result = None
for k in params:
    if params[k] is not None:
        if result is not None:
            result = result + params[k]
        else:
            result = params[k]
if params["PaymentsToAcquireBusinessesNetOfCashAcquired"] is not None and params["PaymentsToAcquireBusinessesGross"] is not None:
    result -= max(params["PaymentsToAcquireBusinessesNetOfCashAcquired"], params["PaymentsToAcquireBusinessesGross"])
    
#mg_interest_net
params="""{"InterestIncomeExpenseNet":null,
"InterestIncome":null,
"InterestExpense":null}"""
result = params["InterestIncomeExpenseNet"]
if result is None:
    if params["InterestIncome"] is not None and params["InterestExpense"] is not None:
        result = params["InterestIncome"] - params["InterestExpense"]