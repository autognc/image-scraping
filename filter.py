import glob
import json
import os
import shutil
import tqdm

IN_PATH = "/home/black/TSL/nasa_images"
OUT_PATH = "/home/black/TSL/nasa_images_filtered"
# my filtering workflow is pretty much "run filtering -> manually review images for bad ones that shouldn't be there ->
# figure out what labels/keywords they have in common -> add those to BAD_LABELS and BAD_KEYWORDS -> run filtering
# again and make sure it didn't get rid of any good images"
GOOD_LABELS = {"Outer Space"}
BAD_LABELS = {"Person", "Text", "Art", "Moon"}
BAD_KEYWORDS = {
    "artwork",
    "artist",
    "computer-generated",
    "computer generated",
    "liftoff",
    "lifts off",
    "lift off",
    "lift-off",
    "moon",
}


def main():
    shutil.rmtree(OUT_PATH)
    os.mkdir(OUT_PATH)
    image_fns = sorted(glob.glob(os.path.join(IN_PATH, "image_*")))
    meta_fns = sorted(glob.glob(os.path.join(IN_PATH, "meta_*")))
    for image_fn, meta_fn in tqdm.tqdm(list(zip(image_fns, meta_fns))):
        with open(meta_fn, "r") as f:
            meta = json.load(f)
        good_confs = [
            l["Confidence"] for l in meta["labels"] if l["Name"] in GOOD_LABELS
        ]
        bad_confs = [l["Confidence"] for l in meta["labels"] if l["Name"] in BAD_LABELS]

        # Filters based on presence of labels and keywords. Labels are from the AWS Rekognition API; the confidence
        # threshold of 30% is an arbitrary one I chose. Keywords are checked against the title and description fields
        # which come directly from the NASA Image API.
        if (
            good_confs
            and max(good_confs) > 30
            and (not bad_confs or max(bad_confs) < 30)
            and (
                "description" not in meta
                or all(k not in meta["description"].lower() for k in BAD_KEYWORDS)
            )
            and (
                "title" not in meta
                or all(k not in meta["title"].lower() for k in BAD_KEYWORDS)
            )
        ):
            shutil.copy(image_fn, os.path.join(OUT_PATH, os.path.basename(image_fn)))
            shutil.copy(meta_fn, os.path.join(OUT_PATH, os.path.basename(meta_fn)))


if __name__ == "__main__":
    main()
