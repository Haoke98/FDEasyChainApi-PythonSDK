import datetime
import json
from typing import Any, Dict

class EasyChainException(Exception):
    """易链SDK基础异常类"""
    def __init__(self, error_code, error_msg=None, request=None, response=None):
        self.error_code = error_code
        self.error_msg = error_msg
        self.request = request
        self.response = response
        
        # 收集额外的调试信息
        self.debug_info = {
            'timestamp': datetime.datetime.now().isoformat(),
            'request_method': getattr(request, 'method', None),
            'request_url': getattr(request, 'url', None),
            'request_headers': dict(getattr(request, 'headers', {})),
            'response_status': getattr(response, 'status_code', None),
            'response_headers': dict(getattr(response, 'headers', {})),
        }
        
        # 处理请求体
        self._add_request_body()
        # 处理响应体
        self._add_response_body()

    def _format_json(self, data: Any, indent: int = 2) -> str:
        """格式化JSON数据"""
        try:
            if isinstance(data, (str, bytes)):
                # 尝试解析JSON字符串
                parsed_data = json.loads(data)
            else:
                parsed_data = data
            return json.dumps(parsed_data, ensure_ascii=False, indent=indent)
        except (json.JSONDecodeError, TypeError):
            return str(data)

    def _add_request_body(self):
        """处理请求体格式"""
        if not self.request:
            return
            
        # 获取请求体
        body = getattr(self.request, 'body', None)
        if not body:
            return
            
        # 检查Content-Type
        headers = dict(getattr(self.request, 'headers', {}))
        content_type = headers.get('Content-Type', '').lower()
        
        if 'application/json' in content_type:
            try:
                # 如果是字节串，先解码
                if isinstance(body, bytes):
                    body = body.decode('utf-8')
                    
                # 如果是字符串形式的查询参数，尝试转换为字典
                if '=' in body and '&' in body:
                    params = {}
                    for param in body.split('&'):
                        key, value = param.split('=')
                        params[key] = value
                    self.debug_info['request_body'] = self._format_json(params)
                else:
                    # 直接作为JSON格式化
                    self.debug_info['request_body'] = self._format_json(body)
            except Exception:
                # 如果处理失败，使用原始形式
                self.debug_info['request_body'] = str(body)
        else:
            # 其他格式，确保是字符串且不过长
            body_str = str(body)
            if len(body_str) > 1000:  # 如果过长，截断显示
                self.debug_info['request_body'] = body_str[:1000] + '...(已截断)'
            else:
                self.debug_info['request_body'] = body_str

    def _add_response_body(self):
        """处理响应体格式"""
        if not self.response:
            return
            
        body = getattr(self.response, 'text', None)
        if not body:
            return
            
        # 检查Content-Type
        headers = dict(getattr(self.response, 'headers', {}))
        content_type = headers.get('Content-Type', '').lower()
        
        if 'application/json' in content_type:
            # JSON格式化
            self.debug_info['response_body'] = self._format_json(body)
        else:
            # 其他格式，确保不过长
            if len(body) > 1000:  # 如果过长，截断显示
                self.debug_info['response_body'] = body[:1000] + '...(已截断)'
            else:
                self.debug_info['response_body'] = body

    def __str__(self):
        # 定义字段显示顺序
        field_order = [
            # Request相关信息
            'timestamp',
            'request_method',
            'request_url',
            'request_headers',
            'request_body',
            # Response相关信息
            'response_status',
            'response_headers',
            'response_body'
        ]
        
        # 构建格式化的调试信息字符串
        debug_parts = []
        for field in field_order:
            v = self.debug_info.get(field)
            if v is not None and v != {} and v != '':
                if isinstance(v, dict):
                    # 字典类型（如headers）进行格式化
                    formatted_value = self._format_json(v)
                    debug_parts.append(f"- {field}:\n{formatted_value}")
                else:
                    # 其他类型直接添加
                    debug_parts.append(f"- {field}: {v}")
        
        debug_str = '\n'.join(debug_parts)
        
        return (
            f"错误码: {self.error_code}\n"
            f"错误信息: {self.error_msg}\n"
            f"调试信息:\n{debug_str}"
        )

    def get_debug_info(self):
        """获取完整的调试信息"""
        return self.debug_info

class AuthenticationError(EasyChainException):
    """401 认证错误"""
    pass

class NotFoundError(EasyChainException):
    """404 资源不存在错误"""
    pass

class ForbiddenError(EasyChainException):
    """403 禁止访问错误"""
    pass

class ServerError(EasyChainException):
    """500 服务器错误"""
    pass

class ServiceUnavailableError(EasyChainException):
    """503 服务暂时不可用错误"""
    pass

# 错误码映射
ERROR_MAPPINGS = {
    401: AuthenticationError,
    403: ForbiddenError,
    404: NotFoundError,
    500: ServerError,
    503: ServiceUnavailableError
}

def create_exception(status_code, message=None, request=None, response=None):
    """
    根据状态码创建对应的异常实例
    
    Args:
        status_code (int): HTTP状态码
        message (str, optional): 错误信息
        request (Request, optional): 请求对象
        response (Response, optional): 响应对象
        
    Returns:
        EasyChainException: 对应的异常实例
    """
    exception_cls = ERROR_MAPPINGS.get(status_code, EasyChainException)
    return exception_cls(status_code, message, request, response)
