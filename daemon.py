#!/usr/bin/env python3

import asyncio
import concurrent.futures
import functools
import logging
import queue
import signal
import json
import os
import string
import threading
import sys
import time
import uuid
import attr
import random

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
)

TOPIC = "Business: TopicName"
PROJECT = "Generic Project"
CHOICES = []
GLOBAL_QUEUE = asyncio.Queue()
THREADS = set()

@attr.s
class Message:
    instance_name = attr.ib()
    message_id    = attr.ib(repr=False)
    hostname      = attr.ib(repr=False, init=False)
    restarted     = attr.ib(repr=False, default=False)
    saved         = attr.ib(repr=False, default=False)
    acked         = attr.ib(repr=False, default=False)
    extended_cnt  = attr.ib(repr=False, default=False)
    def __attrs_post_init__(self):
        self.hostname = f'{self.instance_name}.hostname.net'


async def serialize_tasks(queue):
    """Placeholder for synchronous action"""
    choices = string.ascii_lowercase + string.digits

    while True:
        msgID = str(uuid.uuid4())
        hostID = "".join(random.choices(choices, k=4))
        instance_name = f"Task[{hostID}]"
        msg = Message(msgID, instance_name)
        asyncio.create_task(queue.put(msg))
        logging.debug(f"Initializing task: {msg}")

class RestartFailed(Exception):
    def __init__(self, error, err_msg="Restart failed!"):
        self.error = error
        self.err_msg = err_msg

    def __str__(self):
        return f"{self.__class__}::{self.error} - {self.err_msg}"

class SaveFailed(Exception):
    def __init__(self, error, err_msg="Save failed!"):
        self.error = error
        self.err_msg = err_msg

    def __str__(self):
        return f"{self.__class__}::{self.error} - {self.err_msg}"


async def task_manager(executor, queue):
    logging.info(f"Initialized task execution from jobs in queue.")
    loop = asyncio.get_running_loop()
    asyncio.ensure_future(
        loop.run_in_executor(executor, serialize_tasks, queue), loop=loop
    )

async def restart_host(message):
    """`async def restart_host(message) -> None`: Restart a given host
    
    Args:
    -----
        message (Message): consumed event message
                           for a particular host 
                           to be restarted.
    """
    try:
        # Need to actually define these methods
        _restart_host_here = lambda: None
        _restart_host_here()
        message.restarted = True
        logging.info(f"Restarted host")
    except Exception as e:
        raise RestartFailed("Warning: Failed to restart {message.hostname}")

async def save(message):
    """`async def save(message) -> None`: Saves a message to a database
    Args:
    -----
        message (Message): Consumed event message to be saved.
    """
    # Actually fill out the instructions that define
    # how something gets "saved" in this context
    try:
        save_message = lambda: None
        save_message()
        message.save = True
        logging.info(f"Success! Saved {message} info the database")
    except Exception as e:
        logging.info(f"Warning! Failed  to save message: {message} to the database.")
        raise SaveFailed(e)

async def cleanup(message, event):
    """`async def cleanup(message, event) -> None`: Cleanup tasks related to completing work on a message.
    Args:
    -----
        message (Message): Consumed event message that is being processed.
        event   (asyncio.Event): A queued asyncio event object
    """
    # Block until `event.set()` is called
    await event.wait()

    #
    # IO Operations
    #
    message.acked = True
    logging.info(f"Acknowledged: Cleanup({message})")

class ExtensionError(Exception):
    def __init__(self, excep):
        self.exception = excep
    def __str__(self):
        return f"ExtensionError: Timeout limit exceeded"

async def extend(message, event):
    """`async def extend(message, event) -> None`: Periodically extend the message's acknowledgement deadline.

    Args:
    -----
        msg (Message): consumed event message to extend.
        event (asyncio.Event): event to watch for message extention or
            cleaning up.
    """
    timeout_count=60
    while not event.is_set():
        try:
            assert(timeout_count > 0)
            msg.extended_cnt += 1
            logging.info(f"Extension: extended the deadline by 1.5 seconds for {message}")
            timeout_count -= 1
            await asyncio.sleep(1.5)
        except AssertionError as err:
            logging.warnings("Extension: Grace period has elapsed. Shutting down.")
            raise(ExtensionError(err))

def handle_results(results, message):
    """Handle exception results for a given message."""
    for result in results:
        if isinstance(result, RestartFailed):
            logging.error(f"Restart Failed: retrying for {msg.hostname}")
        elif isinstance(results, SaveFailed):
            logging.error(f"Save Failed Handling Error: {result}")
        elif isinstance(result, ExtensionError) or isinstance(result, Extension):
            logging.error(f"Uncaught problem: {result}")

async def handle_message(message):
    """Launch tasks for a given message"""
    event = asyncio.Event()
    asyncio.create_task(extend(message, event))
    asyncio.create_task(cleanup(message, event))

    results = await asyncio.gather(
            save(message), 
            restart_host(message),
            return_exceptions=True)
    handle_results(results, message)
    event.set()

def consume_sync(queue, loop):
    while True:
        msg = queue.get()
        logging.info(f"Popped: [{msg}]")
        asyncio.create_task(handle_message(msg), loop)

async def consume(executor, queue):
    while True:
        msg = await queue.get()
        logging.info(f"Popped: [{msg}]")
        asyncio.create_task(handle_message(msg))

def handle_exception(executor, loop, context):
    msg = context.get("exception", context["message"])
    logging.error(f"Error: caught exception => {msg}")
    logging.info("Shutting down...")
    asyncio.create_task(shutdown(loop, executor))


async def shutdown(loop, signal=None):
    """Cleanup tasks tied to the service's shutdown."""
    if signal:
        logging.info(f"Received exit signal {signal.name}...")
    logging.info("Closing database connection")
    logging.info("Resolving outstanding messages...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_tasks()]

    [task.cancel() for task in tasks]

    logging.info(f"Notice: Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    logging.info(f"Flushing metrics")
    loop.stop()

async def monitor_tasks():
    while True:
        tasks = [
            t for t in asyncio.all_tasks()
            if t is not asyncio.current_task()
        ]
        [t.print_stack(limit=5) for t in tasks]
        await asyncio.sleep(2)

def main():
    executor = concurrent.futures.ThreadPoolExecutor()
    loop = asyncio.get_event_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT, signal.SIGQUIT)
    for s in signals:
        loop.add_signal_handler(
                s, lambda s=s: asyncio.create_task(shutdown(loop, signal=s))
        )
    #handle_exc_func = functools.partial(handle_exception, executor)
    #loop.set_exception_handler(handle_exc_func)
    q = queue.Queue()

    try:
        loop.create_task(monitor_tasks())
        loop.create_task(task_manager(executor, q))
        loop.create_task(consume(executor, q))
        loop.run_forever()
    finally:
        loop.close()
        logging.info("Successfully shutdown the service manager")

if __name__ == "__main__":
    main()
