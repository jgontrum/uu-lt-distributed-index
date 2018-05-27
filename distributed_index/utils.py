import asyncio

import requests


def requests_wrapper(cmd, *args, **kwargs):
    try:
        response = cmd(*args, **kwargs)
        return response.status_code, response.json()
    except requests.exceptions.ConnectionError:
        return -1, {"message": "Connection refused,"}
    except Exception:
        return -1, {"message": "Unknown error."}


async def run_in_parallel(commands):
    loop = asyncio.get_event_loop()
    futures = [
        loop.run_in_executor(
            None,
            requests_wrapper,
            command['command'],
            *command['args']
        )
        for command in commands
    ]

    return await asyncio.gather(*futures)


def run_on_second_event_loop(commands):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop.run_until_complete(run_in_parallel(commands))
