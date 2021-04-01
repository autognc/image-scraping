from nasa_api import NASA_API
import asyncio
import json
import os
import aiohttp
import glob
import tqdm.asyncio
from aiolimiter import AsyncLimiter
import aioboto3

OUTPUT_PATH = "/home/black/TSL/nasa_images"


async def rekognize(session, rk, pbar, limiter, item):
    """Fetches the image, runs the AWS Rekognition API, and then writes the image and labels to disk"""
    async with limiter:
        # use the first available image size in the following order of priority
        keys = ["medium", "small", "large", "orig", "thumb"]
        key = next(k for k in keys if k in item["image_urls"])
        url = item["image_urls"][key]

        # read the image data
        async with session.get(url) as resp: resp.raise_for_status()
            img = await resp.read()

        # run AWS rekognition
        labels = (
            await rk.detect_labels(Image={"Bytes": img}, MaxLabels=100, MinConfidence=0)
        )["Labels"]

        # write image and labels to output path
        meta = dict(item, labels=labels)
        img_id = item["nasa_id"]
        meta_fn = f"meta_{img_id}.json"
        image_fn = f"image_{img_id}.{url.split('.')[-1]}"
        with open(os.path.join(OUTPUT_PATH, meta_fn), "w") as f:
            json.dump(meta, f)
        with open(os.path.join(OUTPUT_PATH, image_fn), "wb") as f:
            f.write(img)
        pbar.update(1)


async def main():
    limiter = AsyncLimiter(40, 1)
    tasks = set()
    # load the image IDs we've already processed from previous runs
    img_ids = set(
        os.path.basename(s)[6:].split(".")[0]
        for s in glob.glob(os.path.join(OUTPUT_PATH, "image_*"))
    )

    async with NASA_API as napi:
        async with aiohttp.ClientSession() as session:
            async with aioboto3.client("rekognition") as rk:
                # the lower progress par represents the progress of the `rekognize` tasks
                pbar = tqdm.tqdm(position=1, total=0)

                # these search parameters can be changed to get a variety of images
                search = await napi.search(center="JSC", media_type="image", q="dock")

                # the upper progress bar represents the progress of the NASA API search
                async for item in tqdm.asyncio.tqdm(search, position=0):
                    # skip images that have already been processed
                    if item["nasa_id"] not in img_ids:
                        img_ids.add(item["nasa_id"])
                        # enqueue a task that will fetch the image data, run AWS recoknition,
                        # and then write the output to disk
                        tasks.add(
                            asyncio.create_task(
                                rekognize(session, rk, pbar, limiter, item)
                            )
                        )
                        pbar.total += 1

                # wait for all rekognition tasks to finish
                await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
