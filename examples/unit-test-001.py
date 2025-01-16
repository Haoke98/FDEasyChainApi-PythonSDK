import pprint

from FDEasyChainSDK import EasyChainCli

if __name__ == '__main__':
    ddwCli = EasyChainCli(debug=True)
    data1 = ddwCli.company_vc_inv_query("91430104MA4M36MH6R")
    pprint.pprint(data1)
    data2 = ddwCli.company_investment_query("91430104MA4M36MH6R")
    pprint.pprint(data2)
    data3 = ddwCli.company_most_scitech_query("91100000100003962T")
    pprint.pprint(data3)
