page_size = 10000
query_url = "https://data.ntsb.gov/carol-main-public/api/Query/Main"
download_url = "https://data.ntsb.gov/carol-main-public/api/Query/FileExport"
query_body = {"ExportFormat":"data",
              "ResultSetSize":page_size, 
              "ResultSetOffset":0,
              "TargetCollection":"cases",
              "AndOr":"And","QueryGroups":[
                  {"AndOr":"And","QueryRules":[
                      {"RuleType":0,"Values":[],
                       "Columns":["Event.EventDate"],"Operator":"is in the range"}]}],"SortColumn":None,"SortDescending":True,"SessionId":999314}

headers = {"content-type": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36"
        }