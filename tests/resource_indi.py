# -*- coding: utf-8 -*-

import json
import numpy as np
import pandas as pd

from algos.xbrljson import custom_decoder
from settings import Settings
    
class Data():    
    structure = json.loads("""{"bs": {"label": "Consolidated Balance Sheets", "chapter": {"roleuri": "http://www.gm.com/role/ConsolidatedBalanceSheets", "nodes": {"us-gaap:LiabilitiesAndStockholdersEquity": {"name": "us-gaap:LiabilitiesAndStockholdersEquity", "weight": 1.0, "children": {"us-gaap:Liabilities": {"name": "us-gaap:Liabilities", "weight": "1", "children": {"us-gaap:LiabilitiesCurrent": {"name": "us-gaap:LiabilitiesCurrent", "weight": "1", "children": {"us-gaap:AccountsPayableCurrent": {"name": "us-gaap:AccountsPayableCurrent", "weight": "1", "children": null}, "us-gaap:AccruedLiabilitiesCurrent": {"name": "us-gaap:AccruedLiabilitiesCurrent", "weight": "1", "children": {"gm:DealerAndCustomerAllowancesAndClaimsAndDiscountsCurrent": {"name": "gm:DealerAndCustomerAllowancesAndClaimsAndDiscountsCurrent", "weight": "1", "children": null}, "us-gaap:CustomerDepositsCurrent": {"name": "us-gaap:CustomerDepositsCurrent", "weight": "1", "children": null}, "us-gaap:DeferredRevenueCurrent": {"name": "us-gaap:DeferredRevenueCurrent", "weight": "1", "children": null}, "gm:PolicyProductWarrantyRecallCampaignsandCourtesyTransportationAccrualCurrent": {"name": "gm:PolicyProductWarrantyRecallCampaignsandCourtesyTransportationAccrualCurrent", "weight": "1", "children": null}, "gm:AccruedPayrollAndEmployeeBenefitsCurrent": {"name": "gm:AccruedPayrollAndEmployeeBenefitsCurrent", "weight": "1", "children": null}, "us-gaap:OtherAccruedLiabilitiesCurrent": {"name": "us-gaap:OtherAccruedLiabilitiesCurrent", "weight": "1", "children": null}}}, "us-gaap:DebtCurrent": {"name": "us-gaap:DebtCurrent", "weight": "1", "children": null}}}, "us-gaap:LiabilitiesNoncurrent": {"name": "us-gaap:LiabilitiesNoncurrent", "weight": "1", "children": {"us-gaap:OtherPostretirementDefinedBenefitPlanLiabilitiesNoncurrent": {"name": "us-gaap:OtherPostretirementDefinedBenefitPlanLiabilitiesNoncurrent", "weight": "1", "children": null}, "us-gaap:DefinedBenefitPensionPlanLiabilitiesNoncurrent": {"name": "us-gaap:DefinedBenefitPensionPlanLiabilitiesNoncurrent", "weight": "1", "children": null}, "us-gaap:OtherLiabilitiesNoncurrent": {"name": "us-gaap:OtherLiabilitiesNoncurrent", "weight": "1", "children": {"us-gaap:DeferredRevenueNoncurrent": {"name": "us-gaap:DeferredRevenueNoncurrent", "weight": "1", "children": null}, "gm:PolicyProductWarrantyRecallCampaignsandCourtesyTransportationAccrualNoncurrent": {"name": "gm:PolicyProductWarrantyRecallCampaignsandCourtesyTransportationAccrualNoncurrent", "weight": "1", "children": null}, "gm:AccruedEmployeeBenefitsNoncurrent": {"name": "gm:AccruedEmployeeBenefitsNoncurrent", "weight": "1", "children": null}, "gm:PostemploymentBenefitsIncludingFacilityIdlingReservesNoncurrent": {"name": "gm:PostemploymentBenefitsIncludingFacilityIdlingReservesNoncurrent", "weight": "1", "children": null}, "us-gaap:OtherAccruedLiabilitiesNoncurrent": {"name": "us-gaap:OtherAccruedLiabilitiesNoncurrent", "weight": "1", "children": null}}}, "us-gaap:LongTermDebtAndCapitalLeaseObligations": {"name": "us-gaap:LongTermDebtAndCapitalLeaseObligations", "weight": "1", "children": null}}}, "us-gaap:CommitmentsAndContingencies": {"name": "us-gaap:CommitmentsAndContingencies", "weight": "1", "children": null}}}, "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": {"name": "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest", "weight": "1", "children": {"us-gaap:StockholdersEquity": {"name": "us-gaap:StockholdersEquity", "weight": "1", "children": {"us-gaap:CommonStockValue": {"name": "us-gaap:CommonStockValue", "weight": "1", "children": null}, "us-gaap:AdditionalPaidInCapital": {"name": "us-gaap:AdditionalPaidInCapital", "weight": "1", "children": null}, "us-gaap:RetainedEarningsAccumulatedDeficit": {"name": "us-gaap:RetainedEarningsAccumulatedDeficit", "weight": "1", "children": null}, "us-gaap:AccumulatedOtherComprehensiveIncomeLossNetOfTax": {"name": "us-gaap:AccumulatedOtherComprehensiveIncomeLossNetOfTax", "weight": "1", "children": null}}}, "us-gaap:MinorityInterest": {"name": "us-gaap:MinorityInterest", "weight": "1", "children": null}}}}}, "us-gaap:Assets": {"name": "us-gaap:Assets", "weight": 1.0, "children": {"us-gaap:AssetsCurrent": {"name": "us-gaap:AssetsCurrent", "weight": "1", "children": {"us-gaap:CashAndCashEquivalentsAtCarryingValue": {"name": "us-gaap:CashAndCashEquivalentsAtCarryingValue", "weight": "1", "children": null}, "us-gaap:MarketableSecuritiesCurrent": {"name": "us-gaap:MarketableSecuritiesCurrent", "weight": "1", "children": null}, "us-gaap:AccountsNotesAndLoansReceivableNetCurrent": {"name": "us-gaap:AccountsNotesAndLoansReceivableNetCurrent", "weight": "1", "children": null}, "us-gaap:InventoryNet": {"name": "us-gaap:InventoryNet", "weight": "1", "children": {"us-gaap:InventoryWorkInProcessAndRawMaterialsNetOfReserves": {"name": "us-gaap:InventoryWorkInProcessAndRawMaterialsNetOfReserves", "weight": "1", "children": null}, "us-gaap:InventoryFinishedGoodsNetOfReserves": {"name": "us-gaap:InventoryFinishedGoodsNetOfReserves", "weight": "1", "children": null}}}, "gm:AssetsSubjecttoorAvailableforOperatingLeaseNetCurrent": {"name": "gm:AssetsSubjecttoorAvailableforOperatingLeaseNetCurrent", "weight": "1", "children": null}, "us-gaap:OtherAssetsCurrent": {"name": "us-gaap:OtherAssetsCurrent", "weight": "1", "children": null}, "us-gaap:NotesAndLoansReceivableNetCurrent": {"name": "us-gaap:NotesAndLoansReceivableNetCurrent", "weight": "1", "children": null}}}, "us-gaap:AssetsNoncurrent": {"name": "us-gaap:AssetsNoncurrent", "weight": "1", "children": {"us-gaap:EquityMethodInvestments": {"name": "us-gaap:EquityMethodInvestments", "weight": "1", "children": null}, "us-gaap:PropertyPlantAndEquipmentNet": {"name": "us-gaap:PropertyPlantAndEquipmentNet", "weight": "1", "children": {"us-gaap:PropertyPlantAndEquipmentGross": {"name": "us-gaap:PropertyPlantAndEquipmentGross", "weight": "1", "children": null}, "us-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment": {"name": "us-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment", "weight": "-1", "children": null}}}, "us-gaap:IntangibleAssetsNetIncludingGoodwill": {"name": "us-gaap:IntangibleAssetsNetIncludingGoodwill", "weight": "1", "children": null}, "us-gaap:DeferredIncomeTaxAssetsNet": {"name": "us-gaap:DeferredIncomeTaxAssetsNet", "weight": "1", "children": null}, "us-gaap:OtherAssetsNoncurrent": {"name": "us-gaap:OtherAssetsNoncurrent", "weight": "1", "children": null}, "us-gaap:NotesAndLoansReceivableNetNoncurrent": {"name": "us-gaap:NotesAndLoansReceivableNetNoncurrent", "weight": "1", "children": null}, "us-gaap:PropertySubjectToOrAvailableForOperatingLeaseNet": {"name": "us-gaap:PropertySubjectToOrAvailableForOperatingLeaseNet", "weight": "1", "children": null}}}}}}}}, "cf": {"label": "Consolidated Statements Of Cash Flows", "chapter": {"roleuri": "http://www.gm.com/role/ConsolidatedStatementsOfCashFlows", "nodes": {"us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect": {"name": "us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect", "weight": 1.0, "children": {"us-gaap:EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents": {"name": "us-gaap:EffectOfExchangeRateOnCashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents", "weight": "1", "children": null}, "us-gaap:NetCashProvidedByUsedInFinancingActivities": {"name": "us-gaap:NetCashProvidedByUsedInFinancingActivities", "weight": "1", "children": {"us-gaap:NetCashProvidedByUsedInFinancingActivitiesContinuingOperations": {"name": "us-gaap:NetCashProvidedByUsedInFinancingActivitiesContinuingOperations", "weight": "1", "children": {"us-gaap:ProceedsFromRepaymentsOfShortTermDebtMaturingInThreeMonthsOrLess": {"name": "us-gaap:ProceedsFromRepaymentsOfShortTermDebtMaturingInThreeMonthsOrLess", "weight": "1", "children": null}, "us-gaap:ProceedsFromDebtMaturingInMoreThanThreeMonths": {"name": "us-gaap:ProceedsFromDebtMaturingInMoreThanThreeMonths", "weight": "1", "children": null}, "us-gaap:RepaymentsOfDebtMaturingInMoreThanThreeMonths": {"name": "us-gaap:RepaymentsOfDebtMaturingInMoreThanThreeMonths", "weight": "-1", "children": null}, "us-gaap:PaymentsForRepurchaseOfCommonStock": {"name": "us-gaap:PaymentsForRepurchaseOfCommonStock", "weight": "-1", "children": null}, "us-gaap:ProceedsFromIssuanceOfPreferredStockAndPreferenceStock": {"name": "us-gaap:ProceedsFromIssuanceOfPreferredStockAndPreferenceStock", "weight": "1", "children": null}, "us-gaap:PaymentsOfDividends": {"name": "us-gaap:PaymentsOfDividends", "weight": "-1", "children": null}, "us-gaap:ProceedsFromPaymentsForOtherFinancingActivities": {"name": "us-gaap:ProceedsFromPaymentsForOtherFinancingActivities", "weight": "1", "children": null}}}, "us-gaap:CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations": {"name": "us-gaap:CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations", "weight": "1", "children": null}}}, "us-gaap:NetCashProvidedByUsedInInvestingActivities": {"name": "us-gaap:NetCashProvidedByUsedInInvestingActivities", "weight": "1", "children": {"us-gaap:NetCashProvidedByUsedInInvestingActivitiesContinuingOperations": {"name": "us-gaap:NetCashProvidedByUsedInInvestingActivitiesContinuingOperations", "weight": "1", "children": {"us-gaap:PaymentsToAcquirePropertyPlantAndEquipment": {"name": "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment", "weight": "-1", "children": null}, "us-gaap:PaymentsToAcquireAvailableForSaleSecurities": {"name": "us-gaap:PaymentsToAcquireAvailableForSaleSecurities", "weight": "-1", "children": null}, "us-gaap:PaymentsToAcquireTradingSecuritiesHeldforinvestment": {"name": "us-gaap:PaymentsToAcquireTradingSecuritiesHeldforinvestment", "weight": "-1", "children": null}, "us-gaap:ProceedsFromSaleAndMaturityOfAvailableForSaleSecurities": {"name": "us-gaap:ProceedsFromSaleAndMaturityOfAvailableForSaleSecurities", "weight": "1", "children": null}, "us-gaap:ProceedsFromSaleOfTradingSecuritiesHeldforinvestment": {"name": "us-gaap:ProceedsFromSaleOfTradingSecuritiesHeldforinvestment", "weight": "1", "children": null}, "us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired": {"name": "us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired", "weight": "-1", "children": null}, "us-gaap:PaymentsToAcquireFinanceReceivables": {"name": "us-gaap:PaymentsToAcquireFinanceReceivables", "weight": "-1", "children": null}, "us-gaap:ProceedsFromCollectionOfFinanceReceivables": {"name": "us-gaap:ProceedsFromCollectionOfFinanceReceivables", "weight": "1", "children": null}, "us-gaap:PaymentsToAcquireLeasesHeldForInvestment": {"name": "us-gaap:PaymentsToAcquireLeasesHeldForInvestment", "weight": "-1", "children": null}, "us-gaap:ProceedsFromLeasesHeldForInvestment": {"name": "us-gaap:ProceedsFromLeasesHeldForInvestment", "weight": "1", "children": null}, "us-gaap:PaymentsForProceedsFromOtherInvestingActivities": {"name": "us-gaap:PaymentsForProceedsFromOtherInvestingActivities", "weight": "-1", "children": null}}}, "us-gaap:CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations": {"name": "us-gaap:CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations", "weight": "1", "children": null}}}, "us-gaap:NetCashProvidedByUsedInOperatingActivities": {"name": "us-gaap:NetCashProvidedByUsedInOperatingActivities", "weight": "1", "children": {"us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations": {"name": "us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations", "weight": "1", "children": {"us-gaap:IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest": {"name": "us-gaap:IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest", "weight": "1", "children": null}, "gm:DepreciationandImpairmentofEquipmentonOperatingLeasesNet": {"name": "gm:DepreciationandImpairmentofEquipmentonOperatingLeasesNet", "weight": "1", "children": null}, "gm:Depreciationamortizationandimpairmentcharges": {"name": "gm:Depreciationamortizationandimpairmentcharges", "weight": "1", "children": null}, "gm:ForeignCurrencyRemeasurementandTransactionGainsLosses": {"name": "gm:ForeignCurrencyRemeasurementandTransactionGainsLosses", "weight": "-1", "children": null}, "us-gaap:IncomeLossFromEquityMethodInvestmentsNetOfDividendsOrDistributions": {"name": "us-gaap:IncomeLossFromEquityMethodInvestmentsNetOfDividendsOrDistributions", "weight": "-1", "children": null}, "us-gaap:PensionAndOtherPostretirementBenefitContributions": {"name": "us-gaap:PensionAndOtherPostretirementBenefitContributions", "weight": "-1", "children": null}, "us-gaap:PensionAndOtherPostretirementBenefitExpense": {"name": "us-gaap:PensionAndOtherPostretirementBenefitExpense", "weight": "1", "children": null}, "us-gaap:DeferredIncomeTaxExpenseBenefit": {"name": "us-gaap:DeferredIncomeTaxExpenseBenefit", "weight": "1", "children": {"us-gaap:DeferredFederalIncomeTaxExpenseBenefit": {"name": "us-gaap:DeferredFederalIncomeTaxExpenseBenefit", "weight": "1", "children": null}, "us-gaap:DeferredStateAndLocalIncomeTaxExpenseBenefit": {"name": "us-gaap:DeferredStateAndLocalIncomeTaxExpenseBenefit", "weight": "1", "children": null}, "us-gaap:DeferredForeignIncomeTaxExpenseBenefit": {"name": "us-gaap:DeferredForeignIncomeTaxExpenseBenefit", "weight": "1", "children": null}}}, "us-gaap:IncreaseDecreaseInOtherOperatingCapitalNet": {"name": "us-gaap:IncreaseDecreaseInOtherOperatingCapitalNet", "weight": "-1", "children": {"us-gaap:IncreaseDecreaseInAccountsReceivable": {"name": "us-gaap:IncreaseDecreaseInAccountsReceivable", "weight": "1", "children": null}, "gm:Increasedecreaseinpurchasesofwholesalereceivablesnet": {"name": "gm:Increasedecreaseinpurchasesofwholesalereceivablesnet", "weight": "1", "children": null}, "us-gaap:IncreaseDecreaseInInventories": {"name": "us-gaap:IncreaseDecreaseInInventories", "weight": "1", "children": null}, "gm:IncreaseDecreaseinAssetSubjectToOrAvailableForOperatingLeaseNetCurrent": {"name": "gm:IncreaseDecreaseinAssetSubjectToOrAvailableForOperatingLeaseNetCurrent", "weight": "1", "children": null}, "us-gaap:IncreaseDecreaseInOtherOperatingAssets": {"name": "us-gaap:IncreaseDecreaseInOtherOperatingAssets", "weight": "1", "children": null}, "us-gaap:IncreaseDecreaseInAccountsPayable": {"name": "us-gaap:IncreaseDecreaseInAccountsPayable", "weight": "-1", "children": null}, "us-gaap:IncreaseDecreaseInAccruedIncomeTaxesPayable": {"name": "us-gaap:IncreaseDecreaseInAccruedIncomeTaxesPayable", "weight": "-1", "children": null}, "us-gaap:IncreaseDecreaseInAccruedLiabilitiesAndOtherOperatingLiabilities": {"name": "us-gaap:IncreaseDecreaseInAccruedLiabilitiesAndOtherOperatingLiabilities", "weight": "-1", "children": null}}}, "us-gaap:OtherOperatingActivitiesCashFlowStatement": {"name": "us-gaap:OtherOperatingActivitiesCashFlowStatement", "weight": "1", "children": null}}}, "us-gaap:CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations": {"name": "us-gaap:CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations", "weight": "1", "children": null}}}}}}}}, "is": {"label": "Consolidated Income Statements", "chapter": {"roleuri": "http://www.gm.com/role/ConsolidatedIncomeStatements", "nodes": {"us-gaap:EarningsPerShareBasic": {"name": "us-gaap:EarningsPerShareBasic", "weight": 1.0, "children": {"us-gaap:IncomeLossFromContinuingOperationsPerBasicShare": {"name": "us-gaap:IncomeLossFromContinuingOperationsPerBasicShare", "weight": "1", "children": null}, "us-gaap:DiscontinuedOperationIncomeLossFromDiscontinuedOperationNetOfTaxPerBasicShare": {"name": "us-gaap:DiscontinuedOperationIncomeLossFromDiscontinuedOperationNetOfTaxPerBasicShare", "weight": "1", "children": null}}}, "us-gaap:NetIncomeLoss": {"name": "us-gaap:NetIncomeLoss", "weight": 1.0, "children": {"us-gaap:ProfitLoss": {"name": "us-gaap:ProfitLoss", "weight": "1", "children": {"us-gaap:IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest": {"name": "us-gaap:IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest", "weight": "1", "children": {"us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest": {"name": "us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest", "weight": "1", "children": {"us-gaap:OperatingIncomeLoss": {"name": "us-gaap:OperatingIncomeLoss", "weight": "1", "children": {"us-gaap:Revenues": {"name": "us-gaap:Revenues", "weight": "1", "children": null}, "us-gaap:OperatingExpenses": {"name": "us-gaap:OperatingExpenses", "weight": "-1", "children": {"us-gaap:CostOfGoodsAndServicesSold": {"name": "us-gaap:CostOfGoodsAndServicesSold", "weight": "1", "children": null}, "us-gaap:OperatingCostsAndExpenses": {"name": "us-gaap:OperatingCostsAndExpenses", "weight": "1", "children": null}, "us-gaap:SellingGeneralAndAdministrativeExpense": {"name": "us-gaap:SellingGeneralAndAdministrativeExpense", "weight": "1", "children": null}}}}}, "gm:AutomotiveInterestExpense": {"name": "gm:AutomotiveInterestExpense", "weight": "-1", "children": null}, "us-gaap:NonoperatingIncomeExpense": {"name": "us-gaap:NonoperatingIncomeExpense", "weight": "1", "children": {"gm:PensionandOtherPostretirementBenefitsNonoperatingIncome": {"name": "gm:PensionandOtherPostretirementBenefitsNonoperatingIncome", "weight": "1", "children": null}, "gm:ReevaluationofInvestmentsIncomeExpense": {"name": "gm:ReevaluationofInvestmentsIncomeExpense", "weight": "1", "children": null}, "gm:LicensingIncomeNonoperating": {"name": "gm:LicensingIncomeNonoperating", "weight": "1", "children": null}, "us-gaap:InvestmentIncomeNonoperating": {"name": "us-gaap:InvestmentIncomeNonoperating", "weight": "1", "children": null}, "us-gaap:OtherNonoperatingIncomeExpense": {"name": "us-gaap:OtherNonoperatingIncomeExpense", "weight": "1", "children": null}}}, "us-gaap:IncomeLossFromEquityMethodInvestments": {"name": "us-gaap:IncomeLossFromEquityMethodInvestments", "weight": "1", "children": null}}}, "us-gaap:IncomeTaxExpenseBenefit": {"name": "us-gaap:IncomeTaxExpenseBenefit", "weight": "-1", "children": {"us-gaap:DeferredIncomeTaxExpenseBenefit": {"name": "us-gaap:DeferredIncomeTaxExpenseBenefit", "weight": "1", "children": {"us-gaap:DeferredFederalIncomeTaxExpenseBenefit": {"name": "us-gaap:DeferredFederalIncomeTaxExpenseBenefit", "weight": "1", "children": null}, "us-gaap:DeferredStateAndLocalIncomeTaxExpenseBenefit": {"name": "us-gaap:DeferredStateAndLocalIncomeTaxExpenseBenefit", "weight": "1", "children": null}, "us-gaap:DeferredForeignIncomeTaxExpenseBenefit": {"name": "us-gaap:DeferredForeignIncomeTaxExpenseBenefit", "weight": "1", "children": null}}}, "us-gaap:CurrentIncomeTaxExpenseBenefit": {"name": "us-gaap:CurrentIncomeTaxExpenseBenefit", "weight": "1", "children": {"us-gaap:CurrentFederalTaxExpenseBenefit": {"name": "us-gaap:CurrentFederalTaxExpenseBenefit", "weight": "1", "children": null}, "us-gaap:CurrentStateAndLocalTaxExpenseBenefit": {"name": "us-gaap:CurrentStateAndLocalTaxExpenseBenefit", "weight": "1", "children": null}, "us-gaap:CurrentForeignTaxExpenseBenefit": {"name": "us-gaap:CurrentForeignTaxExpenseBenefit", "weight": "1", "children": null}}}}}}}, "us-gaap:IncomeLossFromDiscontinuedOperationsNetOfTax": {"name": "us-gaap:IncomeLossFromDiscontinuedOperationsNetOfTax", "weight": "1", "children": {"gm:DiscontinuedOperationTotalIncomeLossfromDiscontinuedOperationbeforeIncomeTax": {"name": "gm:DiscontinuedOperationTotalIncomeLossfromDiscontinuedOperationbeforeIncomeTax", "weight": "1", "children": {"us-gaap:DiscontinuedOperationIncomeLossFromDiscontinuedOperationBeforeIncomeTax": {"name": "us-gaap:DiscontinuedOperationIncomeLossFromDiscontinuedOperationBeforeIncomeTax", "weight": "1", "children": {"us-gaap:DisposalGroupIncludingDiscontinuedOperationRevenue": {"name": "us-gaap:DisposalGroupIncludingDiscontinuedOperationRevenue", "weight": "1", "children": {"gm:DisposalGroupIncludingDiscontinuedOperationSalesRevenueNet": {"name": "gm:DisposalGroupIncludingDiscontinuedOperationSalesRevenueNet", "weight": "1", "children": null}, "gm:DisposalGroupIncludingDiscontinuedOperationFinancialServicesRevenue": {"name": "gm:DisposalGroupIncludingDiscontinuedOperationFinancialServicesRevenue", "weight": "1", "children": null}}}, "us-gaap:DisposalGroupIncludingDiscontinuedOperationCostsOfGoodsSold": {"name": "us-gaap:DisposalGroupIncludingDiscontinuedOperationCostsOfGoodsSold", "weight": "-1", "children": null}, "gm:DisposalGroupIncludingDiscontinuedOperationFinancialServicesCosts": {"name": "gm:DisposalGroupIncludingDiscontinuedOperationFinancialServicesCosts", "weight": "-1", "children": null}, "us-gaap:DisposalGroupIncludingDiscontinuedOperationGeneralAndAdministrativeExpense": {"name": "us-gaap:DisposalGroupIncludingDiscontinuedOperationGeneralAndAdministrativeExpense", "weight": "-1", "children": null}, "gm:DisposalGroupIncludingDiscontinuedOperationOtherIncomeExpense": {"name": "gm:DisposalGroupIncludingDiscontinuedOperationOtherIncomeExpense", "weight": "-1", "children": null}}}, "us-gaap:DiscontinuedOperationGainLossFromDisposalOfDiscontinuedOperationBeforeIncomeTax": {"name": "us-gaap:DiscontinuedOperationGainLossFromDisposalOfDiscontinuedOperationBeforeIncomeTax", "weight": "1", "children": null}}}, "us-gaap:DiscontinuedOperationTaxEffectOfDiscontinuedOperation": {"name": "us-gaap:DiscontinuedOperationTaxEffectOfDiscontinuedOperation", "weight": "-1", "children": null}}}}}, "us-gaap:NetIncomeLossAttributableToNoncontrollingInterest": {"name": "us-gaap:NetIncomeLossAttributableToNoncontrollingInterest", "weight": "-1", "children": null}}}, "us-gaap:EarningsPerShareDiluted": {"name": "us-gaap:EarningsPerShareDiluted", "weight": 1.0, "children": {"us-gaap:IncomeLossFromContinuingOperationsPerDilutedShare": {"name": "us-gaap:IncomeLossFromContinuingOperationsPerDilutedShare", "weight": "1", "children": null}, "us-gaap:DiscontinuedOperationIncomeLossFromDiscontinuedOperationNetOfTaxPerDilutedShare": {"name": "us-gaap:DiscontinuedOperationIncomeLossFromDiscontinuedOperationNetOfTaxPerDilutedShare", "weight": "1", "children": null}}}}}}}""",
                               object_hook=custom_decoder)
    predicted = []
    multi = np.array([[0.65908062, 0.96733175, 0.78166502, 0.21128808],
       [0.10073616, 0.11583329, 0.24884451, 0.61609891],
       [0.31293996, 0.96662104, 0.5782933 , 0.61014078],
       [0.19820131, 0.39637549, 0.94605783, 0.63923534],
       [0.76924951, 0.63227019, 0.18273102, 0.12387746],
       [0.0715591 , 0.92518168, 0.20217443, 0.15854421],
       [0.71004254, 0.82436319, 0.60627402, 0.92739596],
       [0.0674304 , 0.0099256 , 0.00679383, 0.47172799],
       [0.53425271, 0.17080003, 0.55702819, 0.02536723],
       [0.1756507 , 0.6060573 , 0.07808767, 0.17727423],
       [0.47054736, 0.4638716 , 0.66296056, 0.55676667],
       [0.50866543, 0.7349253 , 0.50288749, 0.76872179],
       [0.01782087, 0.62685159, 0.1702717 , 0.71486386],
       [0.83305566, 0.07917618, 0.54626749, 0.714545  ],
       [0.52834101, 0.00446283, 0.24925105, 0.75284393],
       [0.95466087, 0.01858674, 0.1379188 , 0.96942114],
       [0.80190864, 0.79146357, 0.8588635 , 0.60133131],
       [0.08431321, 0.01107726, 0.9948629 , 0.955783  ],
       [0.60705053, 0.97792861, 0.49428408, 0.7701549 ],
       [0.78567805, 0.13714168, 0.2869116 , 0.28756364],
       [0.08613706, 0.36539336, 0.94703479, 0.07197665],
       [0.91055796, 0.73277581, 0.6978526 , 0.58162721],
       [0.00222175, 0.1917719 , 0.97766798, 0.35314124],
       [0.46202517, 0.36430465, 0.45646703, 0.77109055],
       [0.40874807, 0.57148797, 0.97742006, 0.04921324],
       [0.82742101, 0.0151682 , 0.96048699, 0.73776859],
       [0.48193727, 0.58480841, 0.57112936, 0.65924424],
       [0.08750611, 0.22531261, 0.79175343, 0.08800411],
       [0.75890107, 0.22558029, 0.94921282, 0.47951419],
       [0.85605703, 0.85630837, 0.88006661, 0.11975748],
       [0.97529279, 0.92764495, 0.73197473, 0.76430015],
       [0.62538021, 0.92708751, 0.59215442, 0.99537856],
       [0.07422374, 0.17450844, 0.63890822, 0.7133443 ],
       [0.48778335, 0.48796382, 0.88959959, 0.21932915],
       [0.37560782, 0.38787366, 0.89475955, 0.13469153],
       [0.81664572, 0.15729152, 0.51264161, 0.07544135],
       [0.96015265, 0.09666597, 0.02610314, 0.72293421],
       [0.86599374, 0.30845641, 0.72243281, 0.90816722],
       [0.78254203, 0.96553152, 0.56981912, 0.37525734],
       [0.44603402, 0.3836312 , 0.38169035, 0.7729954 ],
       [0.73406201, 0.01919209, 0.5300172 , 0.03053641],
       [0.52020739, 0.54439487, 0.00569058, 0.27679832],
       [0.3137931 , 0.8903239 , 0.11197747, 0.00525458],
       [0.94030872, 0.86908969, 0.75907009, 0.90533772],
       [0.13625258, 0.64789255, 0.02007132, 0.3680066 ],
       [0.1237719 , 0.83178444, 0.31016777, 0.41226208],
       [0.17376994, 0.41278788, 0.29067373, 0.92077172],
       [0.53386292, 0.29555803, 0.53825513, 0.34390975],
       [0.92462433, 0.13659819, 0.54178503, 0.15391583],
       [0.25167049, 0.23914592, 0.51880758, 0.73608756],
       [0.3745865 , 0.99419975, 0.19293449, 0.34881666],
       [0.53302568, 0.16521643, 0.53365977, 0.01141089],
       [0.35240771, 0.67183498, 0.91421184, 0.61773736],
       [0.0597214 , 0.29207273, 0.52306879, 0.94270934],
       [0.52779762, 0.13789278, 0.28446237, 0.66165186],
       [0.54819173, 0.22556647, 0.14989427, 0.57283024],
       [0.630281  , 0.55510349, 0.10470763, 0.12733027],
       [0.17167313, 0.99934586, 0.80053776, 0.46446693],
       [0.11066262, 0.19447012, 0.26997482, 0.18734132],
       [0.65027203, 0.31955733, 0.98084642, 0.99622968]])
    single = np.array([[0.92174584],
       [0.33535441],
       [0.87823679],
       [0.11629446],
       [0.66583987],
       [0.40805013],
       [0.91652178],
       [0.61947327],
       [0.06905907],
       [0.14132174],
       [0.71842928],
       [0.50327858],
       [0.55751108],
       [0.72310343],
       [0.04506211],
       [0.89086671],
       [0.66158749],
       [0.018267  ],
       [0.84088234],
       [0.53965459],
       [0.75573433],
       [0.36936415],
       [0.85528846],
       [0.73979979],
       [0.95547214],
       [0.84337996],
       [0.77805082],
       [0.98916819],
       [0.00556987],
       [0.57605913],
       [0.42451606],
       [0.98189945],
       [0.16571681],
       [0.56689045],
       [0.80612634],
       [0.92219789],
       [0.15100197],
       [0.77432986],
       [0.92597375],
       [0.10193364],
       [0.79759294],
       [0.71079051],
       [0.62228338],
       [0.02561835],
       [0.91594864],
       [0.02502184],
       [0.5065846 ],
       [0.30903411],
       [0.35324613],
       [0.71741067],
       [0.62882304],
       [0.11520817],
       [0.71556246],
       [0.75635519],
       [0.02380791],
       [0.28873839],
       [0.94629228],
       [0.96379512],
       [0.61924254],
       [0.27348195]])
    single_int = (single>0.5).astype(int)
    
    _nums = None
    _structs = None
    
    @staticmethod
    def predict_single_ones(x: np.ndarray):
        return np.ones((x.shape[0], 1))
    
    @staticmethod
    def predict_single_zeros(x):
        return np.zeros((x.shape[0], 1)).reshape(x.shape[0], 1)
    
    @staticmethod
    def predict_single(x: np.ndarray):
        return Data.single[0:x.shape[0], :]
    
    @staticmethod
    def predict_multi(x: np.ndarray):
        return Data.multi[0:x.shape[0], :]
    
    @staticmethod
    def nums():
        if Data._nums is None:
            Data._nums = (pd.read_csv(Settings.app_dir() + 
                                      'tests/resources/indi/nums.csv')
                            .set_index('index'))
        return Data._nums
    
    @staticmethod
    def structs():
        if Data._structs is None:
            df = (pd.read_csv(Settings.app_dir() + 
                             'tests/resources/indi/structs.csv')
                    .set_index('adsh'))
            df['structure'] = df['structure'].apply(
                    lambda x: json.loads(x, object_hook=custom_decoder))
            Data._structs = df
        return Data._structs
        
    
loader_indicators = """
    {
   "mg_r_liabilities": {
      "class_id": 1,
      "fmodel": "liabilities_class_pch_v2019-03-13.h5"
   },
   "mg_r_liabilities_current": {
      "class_id": 1,
      "fmodel": "liabilities_curr_noncurr_v2019-03-13.h5"
   },
   "mg_r_liabilities_noncurrent": {
      "class_id": 0,
      "fmodel": "liabilities_curr_noncurr_v2019-03-13.h5"
   },
   "mg_r_intangibles": {
      "class_id": 1,
      "fmodel": "assets_multiclass_pch_v2019-03-13.h5"
   },
   "mg_r_fixed_assets": {
      "class_id": 2,
      "fmodel": "assets_multiclass_pch_v2019-03-13.h5"
   },
   "mg_r_capitalized_costs": {
      "class_id": 3,
      "fmodel": "assets_multiclass_pch_v2019-03-13.h5"
   },
   "mg_r_assets_current": {
      "class_id": 1,
      "fmodel": "assets_curr_noncurr_pch_v2019-03-13.h5"
   },
   "mg_r_assets_noncurrent": {
      "class_id": 0,
      "fmodel": "assets_curr_noncurr_pch_v2019-03-13.h5"
   },
   "mg_r_income_noncontroling": {
      "class_id": 1,
      "fmodel": "income_st_multiclass_pch_v2019-03-13.h5"
   },
   "mg_r_sales": {
      "class_id": 2,
      "fmodel": "income_st_multiclass_pch_v2019-03-13.h5"
   },
   "mg_r_cost_of_revenue": {
      "class_id": 3,
      "fmodel": "income_st_multiclass_pch_v2019-03-13.h5"
   },
   "mg_r_SGAA_expenses": {
      "class_id": 4,
      "fmodel": "income_st_multiclass_pch_v2019-03-13.h5"
   },
   "mg_r_one_time_events": {
      "class_id": 5,
      "fmodel": "income_st_multiclass_pch_v2019-03-13.h5"
   },
   "mg_r_income_equity_method": {
      "class_id": 6,
      "fmodel": "income_st_multiclass_pch_v2019-03-13.h5"
   },
   "mg_r_unrealized_gain_loss": {
      "class_id": 7,
      "fmodel": "income_st_multiclass_pch_v2019-03-13.h5"
   },
   "mg_r_interest_expenses": {
      "class_id": 8,
      "fmodel": "income_st_multiclass_pch_v2019-03-13.h5"
   },
   "mg_r_dividend_preferred": {
      "class_id": 9,
      "fmodel": "income_st_multiclass_pch_v2019-03-13.h5"
   },
   "mg_r_cash_operating_activities": {
      "class_id": 1,
      "fmodel": "cashflow_st_cashtype_pch_v2019-03-13.h5"
   },
   "mg_r_cash_investing_activities": {
      "class_id": 2,
      "fmodel": "cashflow_st_cashtype_pch_v2019-03-13.h5"
   },
   "mg_r_cash_financing_activities": {
      "class_id": 3,
      "fmodel": "cashflow_st_cashtype_pch_v2019-03-13.h5"
   },
   "mg_r_cash_acquired_realestate": {
      "class_id": 1,
      "fmodel": "cashflow_st_multiclass_pch_v2019-03-14.h5"
   },
   "mg_r_cash_payments_fixed_assets": {
      "class_id": 2,
      "fmodel": "cashflow_st_multiclass_pch_v2019-03-14.h5"
   },
   "mg_r_cash_payments_other_capital": {
      "class_id": 3,
      "fmodel": "cashflow_st_multiclass_pch_v2019-03-14.h5"
   },
   "mg_r_cash_acquired_businesses": {
      "class_id": 4,
      "fmodel": "cashflow_st_multiclass_pch_v2019-03-14.h5"
   },
   "mg_r_cash_dividends_common": {
      "class_id": 5,
      "fmodel": "cashflow_st_multiclass_pch_v2019-03-14.h5"
   },
   "mg_r_cash_dividends_preferred": {
      "class_id": 6,
      "fmodel": "cashflow_st_multiclass_pch_v2019-03-14.h5"
   },
   "mg_r_cash_buybacks": {
      "class_id": 7,
      "fmodel": "cashflow_st_multiclass_pch_v2019-03-14.h5"
   },
   "mg_r_cash_nonrecurring": {
      "class_id": 8,
      "fmodel": "cashflow_st_multiclass_pch_v2019-03-14.h5"
   },
   "mg_r_cash_dividends_minority": {
      "class_id": 9,
      "fmodel": "cashflow_st_multiclass_pch_v2019-03-14.h5"
   }
}"""
   
loader_get_class = [
    ('liabilities_curr_noncurr_v2019-03-13.h5', 
         {'fdict': 'dictionary.csv', 
          'pc': 'pc', 
          'leaf': 1, 
          'multi': 0, 
          'max_len': 40, 
          'start_chapter': 'bs', 
          'start_tag': 'us-gaap:LiabilitiesAndStockholdersEquity'}),
    ('cashflow_st_multiclass_pch_v2019-03-14.h5', 
         {'fdict': 'dictionary.csv', 
          'pc': 'pc', 
          'leaf': 1, 
          'multi': 1, 
          'max_len': 60, 
          'start_chapter': 'cf', 
          'start_tag': None})
    ]

loader_get_filter = [
    ('cashflow_st_cashtype_pch_v2019-03-13.h5',
     {'filter_model': 'all_gaap_tags_binary_v2019-03-13.h5', 'answer_id': 1}),
    ('liabilities_curr_noncurr_v2019-03-13.h5', 
     {'filter_model': 'liabilities_class_pch_v2019-03-13.h5', 'answer_id': 1})
    ]
indicators_p_columns = ['fy', 'adsh', 'pname', 'sname', 
                        'ord', 'o', 'w', 'value', 'class', 'l']
indicators_r_test_cases_simple = [
    ({'leaf': True,
      'value': 123584000000.0,
      'predict': Data.predict_single_ones,
      'start_chapter': 'bs',
      'start_tag': 'us-gaap:LiabilitiesAndStockholdersEquity',
      'class_id': 1
      },),
    ({'leaf': False,
      'value': 123584000000.0,
      'predict': Data.predict_single_ones,
      'start_chapter': 'bs',
      'start_tag': 'us-gaap:LiabilitiesAndStockholdersEquity',
      'class_id': 1
      },),
    ({'leaf': False,
      'value': np.nan,
      'predict': Data.predict_single_ones,
      'start_chapter': 'bs',
      'start_tag': 'us-gaap:LiabilitiesAndStockholdersEquity',
      'class_id': 0
      },),
    ({'leaf': False,
      'value': 85504000000.0,
      'predict': Data.predict_single,
      'start_chapter': 'bs',
      'start_tag': 'us-gaap:LiabilitiesAndStockholdersEquity',
      'class_id': 1
      },),
    ({'leaf': True,
      'value': 68697000000.0,
      'predict': Data.predict_single,
      'start_chapter': 'bs',
      'start_tag': 'us-gaap:LiabilitiesAndStockholdersEquity',
      'class_id': 1
      },),
    ({'leaf': False,
      'value': 38080000000.0,
      'predict': Data.predict_single,
      'start_chapter': 'bs',
      'start_tag': 'us-gaap:LiabilitiesAndStockholdersEquity',
      'class_id': 0
      },),
    ({'leaf': False,
      'value': np.nan,
      'predict': Data.predict_single,
      'start_chapter': 'is',
      'start_tag': 'us-gaap:LiabilitiesAndStockholdersEquity',
      'class_id': 0
      },)
    ]
   
indicators_sd_test_cases_sintax = [
        ('mg_acquired_realestate', 'static'),
        ('mg_capitalized_software', 'dynamic'),
        ('mg_cash_operating_activities', 'static'),
        ('mg_equity', 'dynamic'),
        ('mg_income', 'static'),
        ('mg_intangibles', 'static'),
        ('mg_interest_net', 'static'),
        ('mg_invest_fix_assets', 'static'),
        ('mg_payments_capital', 'static'),
        ('mg_provision_for_losses', 'static'),
        ('mg_restructuring_nonrecurring', 'static'),
        ('mg_roe', 'static'),
        ('mg_roe_average', 'dynamic'),
        ('mg_roe_variance', 'dynamic'),
        ('mg_r_capitalized_costs_d', 'dynamic'),
        ('mg_r_cash_buybacks_yld', 'dynamic'),
        ('mg_r_debt_tangible_assets', 'static'),
        ('mg_r_dividents_yld', 'dynamic'),
        ('mg_r_equity', 'dynamic'),
        ('mg_r_free_cashflow', 'static'),
        ('mg_r_free_cashflow_yld', 'dynamic'),
        ('mg_r_income', 'static'),
        ('mg_r_income_corrected', 'static'),
        ('mg_r_income_corrected_yld', 'dynamic'),
        ('mg_r_income_free_cashflow', 'static'),
        ('mg_r_roe', 'static'),
        ('mg_r_roe_average', 'dynamic'),
        ('mg_r_roe_variance', 'dynamic'),
        ('mg_r_sales_growth', 'dynamic'),
        ('mg_r_sales_growth_average', 'dynamic'),
        ('mg_sales', 'static'),
        ('mg_SGAAExpense', 'static'),
]
