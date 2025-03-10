# FDEasyChainSDK

五度易链SDK - 企业数据查询接口封装

## 安装

## 使用方法

1. 首先配置环境变量:

```bash
APPID=your_app_id
SECRET_KEY=your_secret_key
```

2. 然后使用SDK进行查询:

```python
from FDEasyChainSDK import FDEasyChainSDK

sdk = FDEasyChainSDK(app_id, secret_key)
result = sdk.query_enterprise_license(enterprise_name)
print(result)
```

## 接口对应调用方法封装实现清单

- 搜索
    - [x] 企业模糊搜索
    - [x] 高级筛选
- 工商信息
    - [x] 企业基本信息
    - [x] 股东信息
    - [x] 高管信息
    - [x] 变更记录
    - [x] 企业对外投资
    - [x] 分支机构
    - [x] 企业关系链
- 司法诉讼
    - [x] 行政处罚
    - [x] 失信被执行人
    - [x] 司法协助
    - [x] 司法协助
    - [x] 被执行人
    - [x] 裁判文书
    - [x] 开庭公告
    - [x] 终本案件
    - [x] 立案信息
    - [x] 限制高消费
    - [x] 破产重整
- 知识产权
    - [x] 软件著作权
    - [x] 专利基本信息
    - [x] 作品著作权
    - [x] 商标基本信息
    - [x] 知识产权出质
    - [x] ICP网站备案
- 上市公司
    - [x] 企业主板新三板上市查询
    - [x] 上市公司公告
    - [x] 港股上市
    - [x] 十大流通股东
- 标准信息
    - [x] 企业国家标准信息查询 (company_standard_query)
    - [x] 企业行业标准信息查询 (company_bz_industry_query)
- 企业发展
    - [x] 认证认可 (company_cnca5_query)
    - [x] 新闻舆情 (company_news_query)
    - [x] 上榜榜单 (company_fc_thirdtop_query)
    - [x] 荣誉资质 (company_billboard_golory_query)
    - [x] 科技成果 (company_most_scitech_query)
    - [x] 融资信息 (company_vc_inv_query)
    - [x] 企业年报
- 经营风险
    - [x] 股权质押 (company_impawn_query)
    - [x] 经营异常 (company_case_abnormity_query)
    - [x] 土地抵押 (company_land_mort_query)
    - [x] 动产抵押 (company_mort_info_query)
    - [x] 重大税收违法 (company_tax_case_query)
    - [x] 简易注销 (company_cancel_easy_query)
    - [x] 清算信息 (company_liquidation_query)
    - [x] 欠税信息 (company_tax_arrears_query)
    - [x] 严重违法 (company_case_ywfwt_query)
- 经营信息
    - [x] 纳税信用评级 (company_tax_rating_query)
    - [x] 行政许可 (company_certificate_query)
    - [x] 招聘信息 (company_job_info_query)
    - [x] 抽查检查 (company_case_check_query)
    - [x] 双随机抽查 (company_case_randomcheck_query)
    - [x] 土地转让 (company_mirland_transfer_query)
    - [x] 电信许可 (company_aggre_cert_query)
    - [x] 招投标信息 (company_bid_list_query)

## 发布

#### Generating distribution archives

* Now run this command from the same directory where pyproject.toml is located:
  ```shell
  python setup.py sdist bdist_wheel
  ```
  This command should output a lot of text and once completed should generate two files in the dist directory:
  ```shell
  dist/
  ├── example_package_YOUR_USERNAME_HERE-0.0.1-py3-none-any.whl
  └── example_package_YOUR_USERNAME_HERE-0.0.1.tar.gz
  ```

#### Uploading the distribution archives

* Now that you are registered, you can use twine to upload the distribution packages. You’ll need to install Twine:
  ```shell
  python3 -m pip install --upgrade twine
  ```
* Once installed, run Twine to upload all of the archives under dist:
  ```shell
  python3 -m twine upload --repository testpypi dist/*
  ```

## License

MIT

## 相关连接

* [文档链接](https://api.datadowell.com/market)