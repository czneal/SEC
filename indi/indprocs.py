# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod
from typing import Optional, Set, cast

from indi.types import Nums, Facts, NoneFacts, NoneFact
from indi.types import eph, nanmin, nanmax, nansum, nanprod, assign, Result
from algos.scheme import Chapters
from utils import class_for_name
"""
Collection of calculation procedures

Each class should begin with 'mg_' prefix
Each class should inherit IndicatorStatic or IndicatorDynamic

run_it() parameters
nums - {fy: {'name': value}}
fy - year for calculation
"""


class Indicator(metaclass=ABCMeta):
    def __init__(self, name):
        self.dp = set()
        self.name = name

    def dependencies(self) -> Set[str]:
        return set([name for name in self.dp
                    if not name.startswith('us-gaap')])

    @abstractmethod
    def calc(
            self,
            nums: Nums,
            fy: int,
            s: Chapters) -> Optional[float]:
        pass

    @abstractmethod
    def description(self) -> str:
        pass


class IndicatorProcedural(Indicator):
    def __init__(self, deep: int = 1):
        super().__init__(self.__class__.__name__)
        self.deep = deep

    def description(self) -> str:
        import inspect
        return inspect.getsource(self.__class__)

    def calc(self, nums: Nums, fy: int, s: Chapters) -> Optional[float]:
        return self.run_it(nums, fy)

    def result(self, result) -> Result:
        if result == eph or result is None:
            return None

        return cast(float, result)

    def fill_none(self, nums: Nums, fy: int) -> NoneFacts:
        facts = nums.get(fy, {})
        return {k: facts.get(k, eph) for k in self.dp}

    def get(self, nums: Nums, fy: int, name: str) -> NoneFact:
        return nums.get(fy, {}).get(name, eph)

    @abstractmethod
    def run_it(self, nums: Nums, fy: int) -> Optional[float]:
        pass


class IndicatorStatic(IndicatorProcedural):
    pass

#     @staticmethod
#     def btype():
#         return('static')


class IndicatorDynamic(IndicatorProcedural):
    pass
#     @staticmethod
#     def btype():
#         return('dynamic')


class mg_income(IndicatorStatic):
    "unittested"

    def __init__(self):
        super().__init__()
        self.dp = {
            "us-gaap:NetIncomeLossAvailableToCommonStockholdersDiluted",
            "us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic",
            "us-gaap:NetIncomeLoss",
            "us-gaap:ProfitLoss",
            "mg_capitalized_software"}

    def run_it(self, nums: Nums, fy: int) -> Result:
        facts = self.fill_none(nums, fy)
        result = nanmin(
            [
                facts
                ["us-gaap:NetIncomeLossAvailableToCommonStockholdersDiluted"],
                facts
                ["us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic"]])

        result = assign(result, facts["us-gaap:NetIncomeLoss"])
        result = assign(result, facts["us-gaap:ProfitLoss"])
        result = result + facts["mg_capitalized_software"]

        return self.result(result)


class mg_acquired_realestate(IndicatorStatic):
    "unittested"

    def __init__(self):
        super().__init__()
        self.dp = {
            'us-gaap:PaymentsToAcquireRealEstate',
            'us-gaap:PaymentsToDevelopRealEstateAssets',
            'us-gaap:PaymentsToAcquireRealEstateAndRealEstateJointVentures',
            'us-gaap:PaymentsToAcquireRealEstateHeldForInvestment'}

    def run_it(self, nums: Nums, fy: int) -> Result:
        params = nums.get(fy, {})
        result, count = nansum(params.values())
        if count == 0:
            return None

        return self.result(result)


class mg_capitalized_software(IndicatorDynamic):
    "unittested"

    def __init__(self):
        super().__init__(deep=2)
        self.dp = {'us-gaap:CapitalizedComputerSoftwareNet'}

    def run_it(self, params: Nums, fy: int) -> Result:
        a = params.get(
            fy,
            {}).get(
            'us-gaap:CapitalizedComputerSoftwareNet',
            None)
        b = params.get(
            fy - 1,
            {}).get(
            'us-gaap:CapitalizedComputerSoftwareNet',
            None)

        if a and b:
            return a - b
        else:
            return None


class mg_cash_operating_activities(IndicatorStatic):
    "unittested"

    def __init__(self):
        super().__init__()
        self.dp = {
            'us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
            'us-gaap:NetCashProvidedByUsedInOperatingActivities'}

    def run_it(self, params: Nums, fy: int) -> Result:
        a = params.get(
            fy,
            {}).get(
            'us-gaap:NetCashProvidedByUsedInOperatingActivities',
            eph)
        b = params.get(
            fy, {}).get(
            'us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
            eph)

        result = assign(a, b)

        return self.result(result)


class mg_equity(IndicatorDynamic):
    "unittested"

    def __init__(self):
        super().__init__(deep=2)
        self.dp = {'us-gaap:Assets', 'us-gaap:Liabilities'}

    def run_it(self, params: Nums, fy: int) -> Result:
        a = self.get(params, fy - 1, 'us-gaap:Assets')
        l = self.get(params, fy - 1, 'us-gaap:Liabilities')
        if a != eph and l != eph:
            return self.result(a - l)
        else:
            return None


class mg_intangibles(IndicatorStatic):
    "unittested"

    def __init__(self):
        super().__init__()
        self.dp = {'us-gaap:OtherIntangibleAssetsNet',
                   'us-gaap:Goodwill',
                   'us-gaap:IntangibleAssetsNetIncludingGoodwill'}

    def run_it(self, params: Nums, fy: int) -> Result:
        result = self.get(
            params, fy, "us-gaap:IntangibleAssetsNetIncludingGoodwill")
        if result != eph:
            return self.result(result)

        result = (self.get(params, fy, "us-gaap:Goodwill") +
                  self.get(params, fy, "us-gaap:OtherIntangibleAssetsNet"))

        return self.result(result)


class mg_interest_net(IndicatorStatic):
    def __init__(self):
        super().__init__()
        self.dp = {'us-gaap:InterestExpense',
                   'us-gaap:InterestIncome',
                   'us-gaap:InterestIncomeExpenseNet'}

    def run_it(self, params: Nums, fy: int) -> Result:
        result = self.get(params, fy, "us-gaap:InterestIncomeExpenseNet")
        if result == eph:
            result = (self.get(params, fy, "us-gaap:InterestIncome") -
                      self.get(params, fy, "us-gaap:InterestExpense"))

        return self.result(result)


class mg_invest_fix_assets(IndicatorStatic):
    "unittested"

    def __init__(self):
        super().__init__()
        self.dp = {
            'mg_payments_capital',
            'us-gaap:PaymentsToDevelopSoftware',
            'us-gaap:PaymentsForSoftware',
            'us-gaap:PaymentsToAcquireBusinessesAndInterestInAffiliates',
            'us-gaap:PaymentsToAcquireBusinessesGross',
            'us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired',
            'us-gaap:PaymentsToAcquireSoftware'}

    def run_it(self, params: Nums, fy: int) -> Result:
        facts = params.get(fy, {})
        result, count = nansum(facts.values())
        mx = nanmax(
            [facts.get(
                "us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired", eph),
             facts.get("us-gaap:PaymentsToAcquireBusinessesGross", eph)])

        return self.result(result - mx)


class mg_payments_capital(IndicatorStatic):
    def __init__(self):
        super().__init__()
        self.dp = {'us-gaap:PaymentsForCapitalImprovements',
                   'us-gaap:PaymentsToAcquireBuildings',
                   'us-gaap:PaymentsToAcquirePropertyPlantAndEquipment',
                   'us-gaap:RepaymentsOfLongTermCapitalLeaseObligations',
                   'us-gaap:PaymentsToAcquireIntangibleAssets',
                   'us-gaap:PaymentsToAcquireProductiveAssets',
                   'us-gaap:PaymentsToAcquireOtherProductiveAssets',
                   'us-gaap:PaymentsToAcquireEquipmentOnLease'}

    def run_it(self, params: Nums, fy: int) -> Result:
        facts = self.fill_none(params, fy)
        result, count = nansum(facts.values())

        return self.result(result)


class mg_provision_for_losses(IndicatorStatic):
    def __init__(self):
        super().__init__()
        self.dp = {'us-gaap:ProvisionForLoanAndLeaseLosses',
                   'us-gaap:ProvisionForLoanLeaseAndOtherLosses'}

    def run_it(self, params: Nums, fy: int) -> Result:
        a = self.get(params, fy, "us-gaap:ProvisionForLoanAndLeaseLosses")
        b = self.get(params, fy, "us-gaap:ProvisionForLoanLeaseAndOtherLosses")

        return self.result(nanmax([a, b]))


class mg_restructuring_nonrecurring(IndicatorStatic):
    def __init__(self):
        super().__init__()
        self.dp = {'us-gaap:OtherNonrecurringExpense',
                   'us-gaap:RestructuringCosts',
                   'us-gaap:OtherNonrecurringIncomeExpense',
                   'us-gaap:RestructuringAndRelatedCostIncurredCost',
                   'us-gaap:RestructuringCharges',
                   'us-gaap:RestructuringCostsAndAssetImpairmentCharges'}

    def run_it(self, params: Nums, fy: int) -> Result:
        facts = self.fill_none(params, fy)

        return self.result(nanmax(facts.values()))


class mg_roe(IndicatorStatic):
    def __init__(self):
        super().__init__()
        self.dp = {'mg_equity', 'mg_income'}

    def run_it(self, params: Nums, fy: int) -> Result:
        e = self.get(params, fy, 'mg_equity')
        i = self.get(params, fy, 'mg_income')

        return self.result(e / i)


class mg_roe_average(IndicatorDynamic):
    "unittested"

    def __init__(self):
        super().__init__(deep=-1)
        self.dp = {'mg_roe'}

    def run_it(self, params: Nums, fy: int) -> Result:
        r = [facts['mg_roe'] + 1.0
             for y, facts in params.items() if y <= fy and 'mg_roe' in facts]

        result: NoneFact = eph
        prod, pw = nanprod(r)
        if pw != 0:
            result = pow(abs(prod), 1.0 / pw) - 1.0

        return self.result(result)


class mg_roe_variance(IndicatorDynamic):
    def __init__(self):
        super().__init__(deep=-1)
        self.dp = {'mg_roe', 'mg_roe_average'}

    def run_it(self, params: Nums, fy: int) -> Result:
        ra = params.get(fy, {}).get('mg_roe_average', None)
        if ra is None:
            return None

        r = [(facts['mg_roe'] - ra)**2
             for y, facts in params.items() if y <= fy and 'mg_roe' in facts]
        summ, count = nansum(r)
        result: NoneFact = eph
        if count:
            result = pow(cast(float, summ), 0.5) / count

        return self.result(result)


class mg_r_capitalized_costs_d(IndicatorDynamic):
    def __init__(self):
        super().__init__(deep=2)
        self.dp = {'mg_r_capitalized_costs'}

    def run_it(self, params: Nums, fy: int) -> Result:
        result: Result = None
        try:
            result = (params[fy]['mg_r_capitalized_costs'] -
                      params[fy - 1]['mg_r_capitalized_costs'])
        except KeyError:
            pass

        return self.result(result)


class mg_r_cash_buybacks_yld(IndicatorDynamic):
    def __init__(self):
        super().__init__(deep=-1)
        self.dp = {'mg_r_cash_buybacks'}

    def run_it(self, params: Nums, fy: int) -> Result:
        summ, count = nansum((facts.get('mg_r_cash_buybacks', eph)
                              for y, facts in params.items() if y <= fy))
        return self.result(-summ)


class mg_r_debt_tangible_assets(IndicatorStatic):
    def __init__(self):
        super().__init__()
        self.dp = {'us-gaap:Assets',
                   'mg_r_capitalized_costs',
                   'mg_r_intangibles',
                   'mg_r_liabilities'}

    def run_it(self, params: Nums, fy: int) -> Result:
        facts = self.fill_none(params, fy)
        ns = facts['us-gaap:Assets'] - facts['mg_r_intangibles'] - facts['mg_r_capitalized_costs']
        result = facts['mg_r_liabilities'] / ns

        return self.result(result)


class mg_r_dividents_yld(IndicatorDynamic):
    def __init__(self):
        super().__init__(deep=-1)
        self.dp = {'mg_r_cash_dividends_common'}

    def run_it(self, params: Nums, fy: int) -> Result:
        summ, count = nansum((facts.get('mg_r_cash_dividends_common', eph)
                              for y, facts in params.items() if y <= fy))
        return self.result(-summ)


class mg_r_equity(IndicatorDynamic):
    def __init__(self):
        super().__init__(deep=2)
        self.dp = {'us-gaap:Assets', 'mg_r_liabilities'}

    def run_it(self, params: Nums, fy: int) -> Result:
        result: NoneFact = eph
        try:
            result = params[fy - 1]['us-gaap:Assets'] - \
                params[fy - 1]['mg_r_liabilities']
        except KeyError:
            pass

        return self.result(result)


class mg_r_free_cashflow(IndicatorStatic):
    def __init__(self):
        super().__init__()
        self.dp = {'mg_r_cash_payments_fixed_assets',
                   'mg_r_cash_payments_other_capital',
                   'mg_r_cash_acquired_businesses',
                   'mg_r_interest_expenses',
                   'us-gaap:mg_tax_rate',
                   'mg_r_cash_operating_activities'}

    def run_it(self, params: Nums, fy: int) -> Result:
        facts = self.fill_none(params, fy)
        result = (
            facts['mg_r_cash_operating_activities'] +
            facts['mg_r_interest_expenses'] *
            (1.0 - facts['us-gaap:mg_tax_rate']) -
            facts['mg_r_cash_payments_fixed_assets'] -
            facts['mg_r_cash_payments_other_capital'] -
            facts['mg_r_cash_acquired_businesses'])
        return self.result(result)


class mg_r_free_cashflow_yld(IndicatorDynamic):
    def __init__(self):
        super().__init__(deep=-1)
        self.dp = {'mg_r_free_cashflow'}

    def run_it(self, params: Nums, fy: int) -> Result:
        result, _ = nansum((params.get(y, {}).get('mg_r_free_cashflow', eph)
                            for y in params if y <= fy))

        return self.result(result)


class mg_r_income(IndicatorStatic):
    def __init__(self):
        super().__init__()
        self.dp = {'us-gaap:ProfitLoss',
                   'us-gaap:NetIncomeLoss',
                   'mg_r_capitalized_costs_d',
                   'mg_r_income_noncontroling',
                   'mg_r_dividend_preferred'}

    def run_it(self, params: Nums, fy: int) -> Result:
        facts = self.fill_none(params, fy)
        result = facts["us-gaap:NetIncomeLoss"]
        result = assign(result, facts["us-gaap:ProfitLoss"])
        result = (result - (
            facts["mg_r_capitalized_costs_d"] +
            facts["mg_r_income_noncontroling"] +
            facts["mg_r_dividend_preferred"]))

        return self.result(result)


class mg_r_income_corrected(IndicatorStatic):
    def __init__(self):
        super().__init__()
        self.dp = {'mg_r_income',
                   'mg_r_one_time_events',
                   'mg_r_income_equity_method',
                   'mg_r_unrealized_gain_loss'}

    def run_it(self, params: Nums, fy: int) -> Result:
        facts = self.fill_none(params, fy)
        result = (facts['mg_r_income'] -
                  (facts['mg_r_one_time_events'] +
                   facts['mg_r_income_equity_method'] +
                   facts['mg_r_unrealized_gain_loss']))
        return self.result(result)


class mg_r_income_corrected_yld(IndicatorDynamic):
    def __init__(self):
        super().__init__(deep=-1)
        self.dp = {'mg_r_income_corrected'}

    def run_it(self, params: Nums, fy: int) -> Result:
        result, _ = nansum((params.get(y, {}).get('mg_r_income_corrected', eph)
                            for y in params if y <= fy))

        return self.result(result)


class mg_r_income_free_cashflow(IndicatorStatic):
    def __init__(self):
        super().__init__()
        self.dp = {'mg_r_income_corrected_yld', 'mg_r_free_cashflow_yld'}

    def run_it(self, params: Nums, fy: int) -> Result:
        nom = params.get(fy, {}).get('mg_r_income_corrected_yld', eph)
        denom = params.get(fy, {}).get('mg_r_free_cashflow_yld', eph)

        result: NoneFact = eph
        if denom != 0.0:
            result = (nom / denom)

        return self.result(result)


class mg_r_free_cashflow_income(IndicatorStatic):
    def __init__(self):
        super().__init__()
        self.dp = {'mg_r_income_corrected_yld', 'mg_r_free_cashflow_yld'}

    def run_it(self, params: Nums, fy: int) -> Result:
        denom = params.get(fy, {}).get('mg_r_income_corrected_yld', eph)
        nom = params.get(fy, {}).get('mg_r_free_cashflow_yld', eph)

        result: NoneFact = eph
        if denom != 0.0:
            result = (nom / denom)

        return self.result(result)


class mg_r_roe(IndicatorStatic):
    def __init__(self):
        super().__init__()
        self.dp = {'mg_r_income', 'mg_r_equity'}

    def run_it(self, params: Nums, fy: int) -> Result:
        e = params.get(fy, {}).get('mg_r_equity', eph)
        i = params.get(fy, {}).get('mg_r_income', eph)

        if i != 0.0:
            e = e / i

        return self.result(e)


class mg_r_roe_average(IndicatorDynamic):
    "unittested"

    def __init__(self):
        super().__init__(deep=-1)
        self.dp = {'mg_r_roe'}

    def run_it(self, params: Nums, fy: int) -> Result:
        r = (facts['mg_r_roe'] + 1.0
             for y, facts in params.items()
             if y <= fy and 'mg_r_roe' in facts)
        prod, pw = nanprod(r)
        if pw != 0:
            prod = pow(abs(prod), 1.0 / pw) - 1.0

        return self.result(prod)


class mg_r_roe_variance(IndicatorDynamic):
    def __init__(self):
        super().__init__(deep=-1)
        self.dp = {'mg_r_roe', 'mg_r_roe_average'}

    def run_it(self, params: Nums, fy: int) -> Result:
        avg = params.get(fy, {}).get('mg_r_roe_average', None)
        if avg is None:
            return None

        r = ((facts['mg_r_roe'] - avg)**2
             for y, facts in params.items()
             if y <= fy and 'mg_r_roe' in facts)
        summ, count = nansum(r)
        if count:
            summ = summ / count

        return self.result(summ)


class mg_r_sales_growth(IndicatorDynamic):
    def __init__(self):
        super().__init__(deep=2)
        self.dp = {'mg_r_sales'}

    def run_it(self, params: Nums, fy: int) -> Result:
        sales_fy = params.get(fy, {}).get('mg_r_sales', eph)
        sales_fy_1 = params.get(fy - 1, {}).get('mg_r_sales', eph)

        if sales_fy_1 != 0.0:
            sales_fy = (sales_fy - sales_fy_1) / sales_fy_1

        return self.result(sales_fy)


class mg_r_sales_growth_average(IndicatorDynamic):
    def __init__(self):
        super().__init__(deep=-1)
        self.dp = {'mg_r_sales_growth'}

    def run_it(self, params: Nums, fy: int) -> Result:
        r = ((facts['mg_r_sales_growth'] + 1.0)
             for y, facts in params.items()
             if y <= fy and 'mg_r_sales_growth' in facts)
        prod, pw = nanprod(r)
        if pw:
            prod = pow(abs(prod), 1.0 / pw) - 1.0

        return self.result(prod)


class mg_sales(IndicatorStatic):
    def __init__(self):
        super().__init__()
        self.dp = {
            'us-gaap:Revenues',
            'us-gaap:SalesRevenueNet',
            'us-gaap:SalesRevenueGoodsNet'}

    def run_it(self, params: Nums, fy: int) -> Result:
        facts = self.fill_none(params, fy)
        result = facts["us-gaap:Revenues"]
        result = assign(result, facts["us-gaap:SalesRevenueNet"])
        result = assign(result, facts["us-gaap:SalesRevenueGoodsNet"])

        return self.result(result)


class mg_SGAAExpense(IndicatorStatic):
    def __init__(self):
        super().__init__()
        self.dp = {
            'us-gaap:SellingGeneralAndAdministrativeExpense',
            'us-gaap:SellingAndMarketingExpense',
            'us-gaap:GeneralAndAdministrativeExpense'}

    def run_it(self, params: Nums, fy: int) -> Result:
        facts = self.fill_none(params, fy)

        if facts['us-gaap:SellingGeneralAndAdministrativeExpense'] == eph:
            return self.result(
                facts['us-gaap:SellingAndMarketingExpense'] +
                facts['us-gaap:GeneralAndAdministrativeExpense'])
        else:
            return self.result(
                facts['us-gaap:SellingGeneralAndAdministrativeExpense'])


def create(name: str) -> Indicator:
    return cast(IndicatorProcedural, class_for_name('indi.indprocs', name)())


if __name__ == '__main__':
    print(issubclass(mg_income, IndicatorStatic))
