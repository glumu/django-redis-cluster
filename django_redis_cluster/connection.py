#coding:utf8
from redis._compat import urlparse

from rediscluster import StrictRedisCluster
from rediscluster.connection import ClusterConnectionPool

class ConnectionFactory(object):

    def __init__(self, server, options):
        
        self._server = server
        self._options = options
    
    @property
    def connection(self):
        '''创建连接'''
        
        params = self._make_connection_params()
        pool = self._get_connection_pool(params)
        connection = StrictRedisCluster(connection_pool=pool)
        return connection
    
    
    def _make_connection_params(self):
        '''生成连接需要的参数'''
        kwargs = {}
        
        socket_timeout = self._options.get("SOCKET_TIMEOUT", None)
        if socket_timeout:
            assert isinstance(socket_timeout, (int, float)), \
                "Socket timeout should be float or integer"
            kwargs["socket_timeout"] = socket_timeout
        
        socket_connect_timeout = self._options.get("SOCKET_CONNECT_TIMEOUT", None)
        if socket_connect_timeout:
            assert isinstance(socket_connect_timeout, (int, float)), \
                "Socket connect timeout should be float or integer"
            kwargs["socket_connect_timeout"] = socket_connect_timeout
            
        return kwargs
            
 
    def _get_connection_pool(self, params):
        '''创建连接池'''
        
        cp_params = dict(params)
        cp_params.update({'decode_responses': True})
        startup_nodes = self._parse_startup_nodes()
        pool = ClusterConnectionPool(startup_nodes, **cp_params)
        
        return pool
    
    
    def _parse_startup_nodes(self):
        '''解析所有服务节点'''
        
        startup_nodes = []
        
        for url_string in self._server:
            url = urlparse(url_string)
            if url.hostname:
                startup_nodes.append({'host': url.hostname, 'port': int(url.port or 6379)})
                
        return startup_nodes
    
    
def get_connection_factory(server, options=None):
    
    return ConnectionFactory(server, options)

