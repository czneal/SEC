# -*- coding: utf-8 -*-
"""
Created on Sat May 25 17:07:11 2019

@author: Asus
"""

node_test_cases = [{'filename': 'aal-20181231_cal.xml',
                    'ref_type': 'calculation',
                    'answers': [({"tag": "us-gaap:AociBeforeTaxAttributableToParent"},
                                 'loc_us-gaap_AociBeforeTaxAttributableToParent_11fd0378-46b0-8746-0a63-84e15aa33091'),
                                ({"tag": "us-gaap:IncreaseDecreaseInOtherOperatingCapitalNet"},
                                 'loc_us-gaap_IncreaseDecreaseInOtherOperatingCapitalNet_a9646808-098c-8496-e17a-df690055019e'),
                                ({"tag": "us-gaap:DepositsOnFlightEquipment"},
                                 'loc_us-gaap_DepositsOnFlightEquipment_fc92c3f1-4a68-d94f-46ac-12a299ce71bc'),
                                ({"tag": "aal:RestructuringAndOther"},
                                 'loc_aal_RestructuringAndOther_2cfe4bbf-561a-680d-dc28-300e4dd4e2bb'),
                                ({"tag": "us-gaap:StockholdersEquity"},
                                 'loc_us-gaap_StockholdersEquity_6765f135-8c69-bebc-a116-d59a52a5b2f2')]},
                   {'filename': 'aal-20181231_pre.xml',
                    'ref_type': 'presentation',
                    'answers': [({"tag": "us-gaap:AccumulatedOtherComprehensiveIncomeLossTable"},
                                 'loc_us-gaap_AccumulatedOtherComprehensiveIncomeLossTable_990506579EE04C8520C6E2CFEAA86E10'),
                                ({"tag": "aal:InactivePeriodBeforeExpirationOfMileageCredits"},
                                 'loc_aal_InactivePeriodBeforeExpirationOfMileageCredits_384E4F191CC167DCF52015D65B588752'),
                                ({"tag": "srt:ConsolidatedEntitiesAxis"},
                                 'loc_srt_ConsolidatedEntitiesAxis_D06622A0C4EA4FE02B2B106BF47414FA'),
                                ({"tag": "us-gaap:AccumulatedOtherComprehensiveIncomeLossNetOfTax"},
                                 'loc_us-gaap_AccumulatedOtherComprehensiveIncomeLossNetOfTax_4FB9A30E79943EDE49715A238F14541F'),
                                ({"tag": "us-gaap:RevenuePerformanceObligationDescriptionOfTiming"},
                                 'loc_us-gaap_RevenuePerformanceObligationDescriptionOfTiming_151F28311FF0804600EE875530420D7F')]},
                   {'filename': 'aal-20181231_def.xml',
                    'ref_type': 'definition',
                    'answers': [({"tag": "us-gaap:AccumulatedOtherComprehensiveIncomeLossTable"},
                                 'loc_us-gaap_AccumulatedOtherComprehensiveIncomeLossTable_990506579EE04C8520C6E2CFEAA86E10'),
                                ({"tag": "aal:InactivePeriodBeforeExpirationOfMileageCredits"},
                                 'loc_aal_InactivePeriodBeforeExpirationOfMileageCredits_384E4F191CC167DCF52015D65B588752'),
                                ({"tag": "srt:ConsolidatedEntitiesDomain"},
                                 'loc_srt_ConsolidatedEntitiesDomain_1F6CC0B64673B4CD3139106BF475790F_default'),
                                ({"tag": "us-gaap:ReclassificationOutOfAccumulatedOtherComprehensiveIncomeTable"},
                                 'loc_us-gaap_ReclassificationOutOfAccumulatedOtherComprehensiveIncomeTable_87F052F4AB578785AE835A238EF26EAA'),
                                ({"tag": "us-gaap:RevenuePerformanceObligationDescriptionOfTiming"},
                                 'loc_us-gaap_RevenuePerformanceObligationDescriptionOfTiming_151F28311FF0804600EE875530420D7F')]}]

arc_test_cases = [{'filename': 'aal-20181231_cal.xml',
                   'ref_type': 'calculation',
                   'answers': ["""{"attrib": {"order": 2, "weight": -1, "arcrole": "summation-item", "type": "arc"}, "from": "loc_us-gaap_AccumulatedOtherComprehensiveIncomeLossNetOfTax_798442bf-12e1-d944-3fa1-8b797b67482e", "to": "loc_us-gaap_AociTaxAttributableToParent_c820490e-bb26-e49a-fe40-074d1b68a433"}""",
                               """{"attrib": {"order": 7, "weight": 1, "arcrole": "summation-item", "type": "arc"}, "from": "loc_us-gaap_NetCashProvidedByUsedInInvestingActivities_28655e34-499f-b7bf-c54f-5de0990d0c65", "to": "loc_us-gaap_ProceedsFromSaleOfOtherInvestments_9f709144-b7dd-5955-b719-be794f06106a"}""",
                               """{"attrib": {"order": 4, "weight": 1, "arcrole": "summation-item", "type": "arc"}, "from": "loc_us-gaap_OtherAssetsNoncurrent_ddb0d184-2205-2a10-5b0e-faf2ccd717ae", "to": "loc_us-gaap_OtherAssetsMiscellaneousNoncurrent_6e6e9eef-7cb9-68f9-7118-146b9eedc79b"}""",
                               """{"attrib": {"order": 3, "weight": 1, "arcrole": "summation-item", "type": "arc"}, "from": "loc_us-gaap_PurchaseObligation_b47daf2a-d854-dfa3-d525-85a178db5970", "to": "loc_us-gaap_PurchaseObligationDueInThirdYear_f6b6b6cf-d833-15d8-b984-3052c3d8f47b"}""",
                               """{"attrib": {"order": 1, "weight": 1, "arcrole": "summation-item", "type": "arc"}, "from": "loc_us-gaap_NetCashProvidedByUsedInOperatingActivities_1bd0bd4e-4a6f-b052-029c-0ecb0c520251", "to": "loc_us-gaap_NetIncomeLoss_a10eea14-f4db-3125-d183-77bb54500e95"}"""]},
                  {'filename': 'aal-20181231_pre.xml',
                   'ref_type': 'presentation',
                   'answers': ["""{"attrib": {"order": 1, "preferredLabel": "terseLabel", "arcrole": "parent-child", "type": "arc"}, "from": "loc_us-gaap_AccumulatedOtherComprehensiveIncomeLossTable_990506579EE04C8520C6E2CFEAA86E10", "to": "loc_srt_ConsolidatedEntitiesAxis_846C2104CC7C8BD2E6CAE2CFEAA904A9"}""",
                               """{"attrib": {"order": 1, "preferredLabel": "terseLabel", "arcrole": "parent-child", "type": "arc"}, "from": "loc_us-gaap_FiniteLivedIntangibleAssetsLineItems_211FFC831F48668A45DC148720B86DF0", "to": "loc_us-gaap_AmortizationOfIntangibleAssets_02CB4237763590A5152414B712C7EF78"}""",
                               """{"attrib": {"order": 1, "preferredLabel": "terseLabel", "priority": 2, "arcrole": "parent-child", "type": "arc"}, "from": "loc_us-gaap_AccountingPoliciesAbstract_941507B0B13572E75C948755303A0F51", "to": "loc_aal_OrganizationBasisOfPresentationAndSummaryOfSignificantAccountingPoliciesTable_ADD6A07501CFD6E6E83C8755303DBE73"}""",
                               """{"attrib": {"order": 1, "preferredLabel": "terseLabel", "arcrole": "parent-child", "type": "arc"}, "from": "loc_us-gaap_ReclassificationOutOfAccumulatedOtherComprehensiveIncomeTable_87F052F4AB578785AE835A238EF26EAA", "to": "loc_srt_ConsolidatedEntitiesAxis_79ABAAF0E25A85B8AB6D5A238EF321F8"}""",
                               """{"attrib": {"order": 13, "preferredLabel": "terseLabel", "priority": 2, "arcrole": "parent-child", "type": "arc"}, "from": "loc_aal_OrganizationBasisOfPresentationAndSummaryOfSignificantAccountingPoliciesLineItems_3309BABE65E1EF02A718875530400DF7", "to": "loc_us-gaap_MarketingAndAdvertisingExpense_46315D703471593F1C348755304338CE"}"""]},
                  {'filename': 'aal-20181231_def.xml',
                   'ref_type': 'definition',
                   'answers': ["""{"attrib": {"order": 1, "arcrole": "hypercube-dimension", "type": "arc"}, "from": "loc_us-gaap_AccumulatedOtherComprehensiveIncomeLossTable_990506579EE04C8520C6E2CFEAA86E10", "to": "loc_srt_ConsolidatedEntitiesAxis_846C2104CC7C8BD2E6CAE2CFEAA904A9"}""",
                               """{"attrib": {"order": 1, "arcrole": "domain-member", "type": "arc"}, "from": "loc_us-gaap_FiniteLivedIntangibleAssetsLineItems_211FFC831F48668A45DC148720B86DF0", "to": "loc_us-gaap_AmortizationOfIntangibleAssets_02CB4237763590A5152414B712C7EF78"}""",
                               """{"attrib": {"order": 1, "arcrole": "hypercube-dimension", "type": "arc"}, "from": "loc_aal_OrganizationBasisOfPresentationAndSummaryOfSignificantAccountingPoliciesTable_ADD6A07501CFD6E6E83C8755303DBE73", "to": "loc_srt_ConsolidatedEntitiesAxis_80E28D697BD427C739FE8755303E6D93"}""",
                               """{"attrib": {"order": 1, "arcrole": "dimension-domain", "type": "arc"}, "from": "loc_srt_ConsolidatedEntitiesAxis_79ABAAF0E25A85B8AB6D5A238EF321F8", "to": "loc_srt_ConsolidatedEntitiesDomain_C978E7917DC7781984995A238EF33F8F"}""",
                               """{"attrib": {"order": 13, "arcrole": "domain-member", "type": "arc"}, "from": "loc_aal_OrganizationBasisOfPresentationAndSummaryOfSignificantAccountingPoliciesLineItems_3309BABE65E1EF02A718875530400DF7", "to": "loc_us-gaap_MarketingAndAdvertisingExpense_46315D703471593F1C348755304338CE"}"""]},
                  ]

chapter_test_cases = [
    {
        'filename': 'aal-20181231_cal.xml',
        'ref_type': 'calculation',
        'answers': [{'nodes': 12,
                     'parent_child': ['us-gaap:OtherComprehensiveIncomeLossBeforeTaxPortionAttributableToParent', 'us-gaap:OciBeforeReclassificationsBeforeTaxAttributableToParent'],
                     'roleuri': 'http://www.aa.com/role/AccumulatedOtherComprehensiveLossComponentsOfAccumulatedOtherComprehensiveLossAociDetails',
                     'tag': ['us-gaap:OciBeforeReclassificationsBeforeTaxAttributableToParent', 1.0]},
                    {'nodes': 3,
                     'parent_child': ['us-gaap:OtherComprehensiveIncomeLossNetOfTaxPortionAttributableToParent',
                                      'us-gaap:OtherComprehensiveIncomeLossBeforeTaxPortionAttributableToParent'],
                     'roleuri': 'http://www.aa.com/role/AccumulatedOtherComprehensiveLossComponentsOfAccumulatedOtherComprehensiveLossAociDetailsCalc2',
                     'tag': ['us-gaap:OtherComprehensiveIncomeLossBeforeTaxPortionAttributableToParent', 1.0]},
                    {'nodes': 0,
                     'parent_child': ['', ''],
                     'roleuri': 'http://www.aa.com/role/BasisOfPresentationAndSummaryOfSignificantAccountingPoliciesPolicies',
                     'tag': ['', 1.0]},
                    {'nodes': 0,
                     'parent_child': ['', ''],
                     'roleuri': 'http://www.aa.com/role/BasisOfPresentationAndSummaryOfSignificantAccountingPoliciesOperatingPropertyAndEquipmentDetail',
                     'tag': ['', 1.0]},
                    {'nodes': 3,
                     'parent_child': ['us-gaap:FiniteLivedIntangibleAssetsNet', 'us-gaap:FiniteLivedIntangibleAssetsAccumulatedAmortization'],
                     'roleuri': 'http://www.aa.com/role/BasisOfPresentationAndSummaryOfSignificantAccountingPoliciesFiniteLivedIntangibleAssetsDetail',
                     'tag': ['us-gaap:FiniteLivedIntangibleAssetsAccumulatedAmortization', -1]}]
    },
    {
        'filename': 'aal-20181231_pre.xml',
        'ref_type': 'presentation',
        'answers': [{'nodes': 25,
                     'parent_child': ['us-gaap:EquityAbstract', 'us-gaap:AccumulatedOtherComprehensiveIncomeLossTable'],
                     'roleuri': 'http://www.aa.com/role/AccumulatedOtherComprehensiveLossComponentsOfAccumulatedOtherComprehensiveLossAociDetails',
                     },
                    {'nodes': 11,
                     'parent_child': ['us-gaap:EquityAbstract', 'us-gaap:ReclassificationOutOfAccumulatedOtherComprehensiveIncomeTable'],
                     'roleuri': 'http://www.aa.com/role/AccumulatedOtherComprehensiveLossReclassificationsOutOfAociDetails'
                     },
                    {'nodes': 47,
                     'parent_child': ['us-gaap:AccountingPoliciesAbstract', 'us-gaap:NewAccountingPronouncementsOrChangeInAccountingPrincipleTable'],
                     'roleuri': 'http://www.aa.com/role/BasisOfPresentationAndSummaryOfSignificantAccountingPoliciesRecentAccountingPronouncementsDetails',
                     },
                    {'nodes': 16,
                     'parent_child': ['us-gaap:AccountingPoliciesAbstract', 'us-gaap:AirlineDestinationTable'],
                     'roleuri': 'http://www.aa.com/role/BasisOfPresentationAndSummaryOfSignificantAccountingPoliciesPassengerRevenueByGeographicRegionDetails'
                     },
                    {'nodes': 19,
                     'parent_child': ['us-gaap:AccountingPoliciesAbstract', 'us-gaap:ScheduleOfPropertyPlantAndEquipmentTable'],
                     'roleuri': 'http://www.aa.com/role/BasisOfPresentationAndSummaryOfSignificantAccountingPoliciesOperatingPropertyAndEquipmentDetail'
                     }]
    },
    {
        'filename': 'aal-20181231_def.xml',
        'ref_type': 'definition',
        'answers': [{'nodes': 24,
                     'parent_child': ['us-gaap:AOCIAttributableToParentNetOfTaxRollForward', 'us-gaap:AccumulatedOtherComprehensiveIncomeLossNetOfTax'],
                     'roleuri': 'http://www.aa.com/role/AccumulatedOtherComprehensiveLossComponentsOfAccumulatedOtherComprehensiveLossAociDetails'
                     },
                    {'nodes': 10,
                     'parent_child': ['us-gaap:ReclassificationAdjustmentOutOfAccumulatedOtherComprehensiveIncomeLineItems',
                                      'us-gaap:ReclassificationFromAociCurrentPeriodNetOfTaxAttributableToParent'],
                     'roleuri': 'http://www.aa.com/role/AccumulatedOtherComprehensiveLossReclassificationsOutOfAociDetails',
                     },
                    {'nodes': 46,
                     'parent_child': ['us-gaap:NewAccountingPronouncementsOrChangeInAccountingPrincipleLineItems', 'us-gaap:CumulativeEffectOfNewAccountingPrincipleInPeriodOfAdoption'],
                     'roleuri': 'http://www.aa.com/role/BasisOfPresentationAndSummaryOfSignificantAccountingPoliciesRecentAccountingPronouncementsDetails',
                     },
                    {'nodes': 15,
                     'parent_child': ['us-gaap:AirlineDestinationDisclosureLineItems', 'us-gaap:AirlineDestinationTable'],
                     'roleuri': 'http://www.aa.com/role/BasisOfPresentationAndSummaryOfSignificantAccountingPoliciesPassengerRevenueByGeographicRegionDetails',
                     'tag': ['OtherComprehensiveIncomeLossNetOfTaxPortionAttributableToParent', 1.0]},
                    {'nodes': 18,
                     'parent_child': ['us-gaap:PropertyPlantAndEquipmentLineItems', 'us-gaap:PropertyPlantAndEquipmentSalvageValuePercentage'],
                     'roleuri': 'http://www.aa.com/role/BasisOfPresentationAndSummaryOfSignificantAccountingPoliciesOperatingPropertyAndEquipmentDetail',
                     }]
    }]

dimchapter_test_cases = [
    {'filename': 'aal-20181231_def.xml', 'ref_type': 'definition',
     'answers':
     [[(None, None),
       ("srt:ConsolidatedEntitiesAxis", "srt:SubsidiariesMember"),
       ("srt:ProductOrServiceAxis", "aal:AirTrafficMember"),
       ("srt:ProductOrServiceAxis", "aal:LoyaltyProgramMember")],
      [(None, None),
       ("srt:ConsolidatedEntitiesAxis", "srt:SubsidiariesMember"),
       ("us-gaap:FairValueByMeasurementFrequencyAxis",
        "us-gaap:FairValueMeasurementsRecurringMember"),
       ("us-gaap:FinancialInstrumentAxis", "us-gaap:MoneyMarketFundsMember"),
       ("us-gaap:FinancialInstrumentAxis",
        "us-gaap:CorporateDebtSecuritiesMember"),
       ("us-gaap:FinancialInstrumentAxis", "us-gaap:BankTimeDepositsMember"),
       ("us-gaap:FinancialInstrumentAxis",
        "us-gaap:RepurchaseAgreementsMember"),
       ("us-gaap:FairValueByFairValueHierarchyLevelAxis",
        "us-gaap:FairValueInputsLevel1Member"),
       ("us-gaap:FairValueByFairValueHierarchyLevelAxis",
        "us-gaap:FairValueInputsLevel2Member"),
       ("us-gaap:FairValueByFairValueHierarchyLevelAxis",
        "us-gaap:FairValueInputsLevel3Member"),
       ("us-gaap:InvestmentSecondaryCategorizationAxis",
        "aal:MaturityDatesExceedingOneYearMember"),
       ("srt:ScheduleOfEquityMethodInvestmentEquityMethodInvesteeNameAxis",
        "aal:ChinaSouthernAirlinesCompanyLimitedMember")],
      [None, "srt:ConsolidatedEntitiesAxis", "srt:ProductOrServiceAxis"],
      [None, "srt:ConsolidatedEntitiesAxis",
       "us-gaap:FairValueByMeasurementFrequencyAxis",
       "us-gaap:FinancialInstrumentAxis",
       "us-gaap:FairValueByFairValueHierarchyLevelAxis",
       "us-gaap:InvestmentSecondaryCategorizationAxis",
       "srt:ScheduleOfEquityMethodInvestmentEquityMethodInvesteeNameAxis"]]}]
