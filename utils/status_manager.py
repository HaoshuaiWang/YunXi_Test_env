from typing import Dict, Any
import threading

class StatusManager:
    _instance = None
    _lock = threading.Lock()
    
    @staticmethod
    def get_instance():
        """获取缓存管理器的单例实例"""
        if StatusManager._instance is None:
            with StatusManager._lock:
                if StatusManager._instance is None:
                    StatusManager._instance = StatusManager()
        return StatusManager._instance
    
    def __init__(self):
        if StatusManager._instance is not None:
            raise Exception("请使用 get_instance() 方法获取实例")
        self.cache: Dict[str, Any] = {}
        self.cache_lock = threading.Lock()
    
    def add(self, key: str, value: Any) -> None:
        """添加或更新缓存项"""
        with self.cache_lock:
            self.cache[key] = value
    
    def get(self, key: str) -> Any:
        """获取缓存项"""
        with self.cache_lock:
            return self.cache.get(key)
    
    def get_all(self) -> Dict[str, Any]:
        """获取所有缓存项的副本"""
        with self.cache_lock:
            return dict(self.cache)
    
    def delete(self, key: str) -> bool:
        """删除缓存项，返回是否删除成功"""
        with self.cache_lock:
            if key in self.cache:
                del self.cache[key]
                return True
            return False
    
    def clear(self) -> None:
        """清空所有缓存"""
        with self.cache_lock:
            self.cache.clear()
