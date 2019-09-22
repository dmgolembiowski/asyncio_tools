""" Adapted from the AITools at https://iwalton.com/wiki """
import asyncio
import functools
import logging

def make_dynamic(function):
    cache = {}
    @functools.wraps(function)
    def result(*args, clear_cache=False,
            ignore_cache=False,
            skip_cache=False,
            **kwargs):
        nonlocal cache
        call = (args, tuple(kwargs.items()))
        if clear_cache:
            cache = {}
        if call in cache and not ignore_cache:
            return cache[call]
        res = function(*args, **kwargs)
        if not skip_cache:
            cache[call] = res
        return res
    return result

async def requires_callback():
    """async_dynamic_caching"""
    pass
