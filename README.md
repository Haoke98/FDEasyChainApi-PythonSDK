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

## 主要功能

- 企业行政许可证查询
- 企业股权质押查询
- 企业招投标信息查询
- 企业新闻舆情查询
- 企业上榜榜单查询
- 企业荣誉资质查询
- 企业科技成果查询
- 企业融资信息查询
- 更多查询功能...

## License

MIT