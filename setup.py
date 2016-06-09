#coding: utf8

from setuptools import setup

description = """
Full featured redis cluster cache backend for Django
"""

setup(
    name = "django-redis-cluster",
    url = "https://github.com/glumu/django-redis-cluster",
    author = "Glen",
    author_email = "glumu@126.com",
    version = "1.0.0",
    packages = [
        "django_redis_cluster",
        "django_redis_cluster.client",
        "django_redis_cluster.serializers",
    ],
    description = description.strip(),
    install_requires = [
        "Django>=1.9.6",
        "redis>=2.10.5",
        "redis-py-cluster>=1.2.0",
        "msgpack-python>=0.4.7",
    ],
    zip_safe = False,
    include_package_data = True,
    package_data = {
        "": ["*.html"],
    },
    classifiers = [
        "Development Status :: 5 - Production/Stable",
        "Operating System :: OS Independent",
        "Environment :: Web Environment",
        "Framework :: Django",
        "License :: OSI Approved :: BSD License",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.2",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Topic :: Software Development :: Libraries",
        "Topic :: Utilities",
    ],
)
