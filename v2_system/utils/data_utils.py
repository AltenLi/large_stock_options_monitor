"""
数据处理工具函数
"""

def safe_int_convert(value, default=0):
    """
    安全转换为整数，处理'N/A'等非数字值
    
    Args:
        value: 要转换的值
        default: 转换失败时的默认值，默认为0
        
    Returns:
        int: 转换后的整数值
    """
    if value is None or value == 'N/A' or value == '':
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def safe_float_convert(value, default=0.0):
    """
    安全转换为浮点数，处理'N/A'等非数字值
    
    Args:
        value: 要转换的值
        default: 转换失败时的默认值，默认为0.0
        
    Returns:
        float: 转换后的浮点数值
    """
    if value is None or value == 'N/A' or value == '':
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_str_convert(value, default=''):
    """
    安全转换为字符串，处理None等值
    
    Args:
        value: 要转换的值
        default: 转换失败时的默认值，默认为空字符串
        
    Returns:
        str: 转换后的字符串值
    """
    if value is None:
        return default
    try:
        return str(value)
    except (ValueError, TypeError):
        return default