# Redis cluster cache backend for Django

Django Redis3.0+ 集群缓存应用

由于 [django-redis](https://github.com/niwinz/django-redis) 不支持最新Redis3.0+集群服务，参考之开发了此应用，支持Django依赖Redis cluster作后台缓存服务。

client api refer to [djagno-redis documentation](http://niwinz.github.io/django-redis/latest/).

## Requirement

Python 3.0+

Django>=1.9.6

redis>=2.10.5

redis-py-cluster>=1.2.0

msgpack-python>=0.4.7

## How to install

```
$python setup.py install
or
$pip install django-redis-cluster
```

## Django Settings

>
```
CACHES = {
    'default': {
        'BACKEND': 'django_redis_cluster.cache.RedisClusterCache',
        "LOCATION": [
            "redis://127.0.0.1:7000/0",
            "redis://127.0.0.1:7001/0",
            "redis://127.0.0.1:7002/0",
        ],
        'OPTIONS': {
            "CLIENT_CLASS": "django_redis_cluster.client.DefaultClient",
        }
    }
}
```
