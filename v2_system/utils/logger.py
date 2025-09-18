# SPDX-License-Identifier: GPL-3.0-or-later
# -*- coding: utf-8 -*-
"""
V2系统日志配置模块
"""

import logging
import logging.handlers
import os
import sys

# 添加V2系统路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import SYSTEM_CONFIG


def setup_logger(name='V2OptionMonitor'):
    """V2系统设置日志记录器"""
    
    # 创建日志目录
    log_dir = os.path.dirname(SYSTEM_CONFIG['log_file'])
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    
    # 创建logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, SYSTEM_CONFIG['log_level']))
    
    # 避免重复添加handler
    if logger.handlers:
        return logger
    
    # 创建formatter
    formatter = logging.Formatter(
        SYSTEM_CONFIG['log_format'],
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 控制台handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件handler (带轮转)
    file_handler = logging.handlers.RotatingFileHandler(
        SYSTEM_CONFIG['log_file'],
        maxBytes=SYSTEM_CONFIG['log_max_size'],
        backupCount=SYSTEM_CONFIG['log_backup_count'],
        encoding='utf-8'
    )
    file_handler.setLevel(getattr(logging, SYSTEM_CONFIG['log_level']))
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger
