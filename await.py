

import asyncio
import time


async def do_work(task: str, delay: float = 1.0):
    print(f"{task} started")
    await asyncio.sleep(1.0)
    print(f"{task} done")


async def main():
    start_time = time.perf_counter()

    todo = [1,2,3]

    # # synchronous
    # for item in todo:
    #     await do_work(item)

    # # async tasks
    # tasks = [asyncio.create_task(do_work(item)) for item in todo]
    # done, pending = await asyncio.wait(tasks)
    # for task in done:
    #     result = task.result()

    # async gather tasks
    tasks = [asyncio.create_task(do_work(item)) for item in todo]
    results = await asyncio.gather(*tasks)
    print(results)

    # async gather coroutines
    coros = [do_work(item) for item in todo]
    results = await asyncio.gather(*coros)
    print(results)

    end_time = time.perf_counter()
    print(f"time taken: {end_time - start_time:.2f} s")


if __name__ == "__main__":
    asyncio.run(main())