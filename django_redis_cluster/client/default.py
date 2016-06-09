#coding:utf8
import zlib
import warnings
import socket
from collections import OrderedDict

from django.utils.encoding import smart_text

from django.core.cache.backends.base import get_key_func
from django.core.exceptions import ImproperlyConfigured

try:
    from django.core.cache.backends.base import DEFAULT_TIMEOUT
except ImportError:
    DEFAULT_TIMEOUT = object()
    
try:
    from redis.exceptions import TimeoutError, ResponseError
    _main_exceptions = (TimeoutError, ResponseError, ConnectionError, socket.timeout)
except ImportError:
    _main_exceptions = (ConnectionError, socket.timeout)

from ..serializers.pickle import PickleSerializer
from ..utils import CacheKey, integer_types
from ..exceptions import ConnectionInterrupted
from .. import connection

class DefaultClient(object):
    
    def __init__(self, server, params, backend):
        self._server = server # redis服务
        self._params = params
        self._backend = backend
        
        self.reverse_key = get_key_func(params.get('REVERS_KEY_FUNCTION') or
                                        'django_redis_cluster.utils.default_reverse_key')
        
        if not self._server:
            raise ImproperlyConfigured('Missing connections string')
        
        # 初始空clients
        if not isinstance(self._server, (list, tuple, set)):
            self._server = self._server.split(',')
        
        # 定义配置的压缩解压库
        self._options = params.get('OPTIONS', {})
        self._options.setdefault('COMPRESS_COMPRESSOR', zlib.compress)
        self._options.setdefault('COMPRESS_DECOMPRESSOR', zlib.decompress)
        self._options.setdefault('COMPRESS_DECOMPRESSOR_ERROR', zlib.error)
        
        # 序列化配置
        self._serializer = PickleSerializer(options=self._options)
        self._connect_factory = connection.get_connection_factory(server=self._server, options=self._options)
        self._client = self._connect_factory.connection
        
    
    def __contains__(self, key):
        return self.has_key(key)
    
    
    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        '''添加一个键值，如果存在则返回失败'''
        return self.set(key, value, timeout, version=version, nx=True)
    
    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None, nx=False, xx=False):
        '''缓存一个键值'''
        nkey = self._make_key(key, version=version)
        nvalue = self.encode(value)
        print(nkey, nvalue)
        
        if timeout is True:
            warnings.warn("Using True as timeout value, is now deprecated.", DeprecationWarning)
            timeout = self._backend.default_timeout
        
        if timeout == DEFAULT_TIMEOUT:
            timeout = self._backend.default_timeout
        
        try:
            if timeout is not None:
                if timeout > 0:
                    timeout = int(timeout)
                elif timeout <= 0:
                    if nx:
                        timeout = None
                    else:
                        return self.delete(key, version=version)
            
            return self._client.set(nkey, nvalue, nx=nx, ex=timeout, xx=xx)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=self._client, parent=e)
        
    
    def set_many(self, data, timeout=DEFAULT_TIMEOUT, version=None):
        '''设置多个缓存'''
        
        try:
            pipeline = self._client.pipeline()
            for key, value in data.items():
                self.set(key, value, timeout, version=version)
            pipeline.execute()
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=self._client, parent=e)
        
    
    def get(self, key, default=None, version=None):
        '''如果key存在时取回键值，否则返回默认值'''
            
        key = self._make_key(key, version=version)
        
        try:
            value = self._client.get(key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=self._client, parent=e)
        
        if value is None:
            return default
        
        return self.decode(value)
    
    
    def get_many(self, keys, version=None):
        '''查询多个key缓存'''
        
        if not keys:
            return {}
        
        recovered_data = OrderedDict()
        
        new_keys = [self._make_key(k, version=version) for k in keys]
        map_keys = dict(zip(new_keys, keys))
        
        try:
            results = self._client.mget(*new_keys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=self._client, parent=e)
        
        for key, value in zip(new_keys, results):
            if value is None:
                continue
            recovered_data[map_keys[key]] = self.decode(value)
        return recovered_data
        
        
    def keys(self, search, version=None):
        '''查询缓存的keys'''
        
        pattern = self._make_key(search, version=version)
        try:
            encoding_map = [smart_text(k) for k in self._client.keys(pattern)]
            return [self.reverse_key(k) for k in encoding_map]
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=self._client, parent=e)
        
    
    def delete(self, key, version=None):
        '''删除key的缓存'''
        try:
            self._client.delete(self._make_key(key, version=version))
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=self._client, parent=e)
        
    
    def delete_pattern(self, pattern, version=None):
        '''删除所有符合条件的key'''
        pattern = self._make_key(pattern, version=version)
        
        try:
            count = 0
            for key in self._client.scan_iter(pattern):
                self._client.delete(key)
                count += 1
            return count
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=self._client, parent=e)
        
    
    def delete_many(self, keys, version=None):
        '''一次性删除多个key'''
        
        keys = [self._make_key(k, version=version) for k in keys]
        
        if not keys:
            return 
        
        try:
            return self._client.delete(*keys)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=self._client, parent=e)
        
        
    def has_key(self, key, version=None):
        '''是否存在key'''
        
        key = self._make_key(key, version=version)
        
        try:
            return self._client.exists(key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=self._client, parent=e)
        
    
    def clear(self):
        '''清空所有缓存'''
        self._client.flushdb()
    
    
    def lock(self, key, version=None, timeout=None, sleep=0.1, blocking_timeout=None):
        
        key = self._make_key(key, version)
        return self._client.lock(key, timeout=timeout, sleep=sleep, blocking_timeout=blocking_timeout)
    
    
    def decode(self, value):
        '''解密内容'''
        try:
            value = int(value)
        except (ValueError, TypeError):
            if self._options.get('COMPRESS_MIN_LEN', 0) > 0:
                try:
                    value = self._options['COMPRESS_COMPRESSOR'](value)
                except self._options['COMPRESS_DECOMPRESSOR_ERROR']:
                    pass
            value = self._serializer.loads(value)
        return value
    
    
    def encode(self, value):
        '''加密内容'''
        if isinstance(value, bool) or not isinstance(value, integer_types):
            encoded_value = self._serializer.dumps(value)
            if self._options.get('COMPRESS_MIN_LEN', 0) > 0:
                if len(encoded_value) >= self._options['COMPRESS_MIN_LEN']:
                    compressed = self._options['COMPRESS_COMPRESSOR'](encoded_value)
                    if len(compressed) < len(encoded_value):
                        encoded_value = compressed
            return encoded_value
        
        return value
    
    
    def incr_version(self, key, delta=1, version=None):
        """
        Adds delta to the cache version for the supplied key. Returns the
        new version.
        """
        if version is None:
            version = self._backend.version
            
        old_key = self._make_key(key, version)
        value = self.get(old_key, version=version)
        
        try:
            ttl = self._client.ttl(old_key)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=self._client, parent=e)
        
        if value is None:
            raise ValueError("Key '%s' not found" % key)
    
        if isinstance(key, CacheKey):
            new_key = self._make_key(key.original_key(), version=version + delta)
        else:
            new_key = self._make_key(key, version=version + delta)
            
        self.set(new_key, value, timeout=ttl)
        self.delte(old_key)
        return version + delta
    
    
    def persist(self, key, version=None):
        
        key = self._make_key(key, version=version)
        
        if self._client.exists(key):
            self._client.persist(key)
            
        
    def expire(self, key, timeout, version=None):
        
        key = self._make_key(key, version=version)
        
        if self._client.exists(key):
            self._client.expire(key, timeout)
            
        
    def _incr(self, key, delta=1, version=None):
        
        key = self._make_key(key, version=version)
        
        try:
            if not self._client.exists(key):
                raise ValueError("Key '%s' not found" % key)
            
            try:
                value = self._client.incr(key, delta)
            except ResponseError:
                timeout = self._client.ttl(key)
                value = self.get(key, version=version) + delta
                self.set(key, value, version=version, timeout=timeout)
        except _main_exceptions as e:
            raise ConnectionInterrupted(connection=self._client, parent=e)
        
        return value
    
    
    def incr(self, key, delta=1, version=None):
        """
        Add delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        return self._incr(key=key, delta=delta, version=version)
    
    
    def decr(self, key, delta=1, version=None):
        """
        Decreace delta to value in the cache. If the key does not exist, raise a
        ValueError exception.
        """
        return self._incr(key=key, delta=-delta, version=version)
    
    
    def ttl(self, key, version=None):
        """
        Executes TTL redis command and return the "time-to-live" of specified key.
        If key is a non volatile key, it returns None.
        """
        key = self._make_key(key, version=version)
        if not self._client.exists(key):
            return 0
        
        t = self._client.ttl(key)
        return (t >= 0 and t or None)
    
    
    def iter_keys(self, search, itersize=None, version=None):
        """
        Same as keys, but uses redis >= 2.8 cursors
        for make memory efficient keys iteration.
        """
        
        pattern = self._make_key(search, version=version)
        for item in self._client.scan_iter(match=pattern, count=itersize):
            item = smart_text(item)
            yield self.reverse_key(item)
    
            
    def _make_key(self, key, version=None):
        '''格式化key'''
        if isinstance(key, CacheKey):
            return key
        return CacheKey(self._backend.make_key(key, version))
        
        