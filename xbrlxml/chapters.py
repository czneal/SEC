import json

chapters = {
    "18 Consolidated Balance Sheets": "bs",
    "20 Consolidated Statements of Cash Flows": "cf",
    "21 Consolidated Statements of Operations and Comprehensive Income": "is",
    "23 Consolidated Statements of Shareholders' Equity": "se",

}

print(json.dumps(chapters))
