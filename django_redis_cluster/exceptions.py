#coding:utf8

class ConnectionInterrumped(Exception):
    
    def __init__(self, connection, parent=None):
        self.connection = connection
        self.parent = parent


class ConnectionInterrupted(ConnectionInterrumped):
    '''连接中断异常'''
    
    def __str__(self):
        error_type = "ConnectionInterrupted"
        error_msg = "An error occurred while connecting to redis cluster"
        
        if self.parent:
            error_type = self.parent.__class__.__name__
            error_msg = str(self.parent)
        
        return "Redis Cluster %s: %s" % (error_type, error_msg)