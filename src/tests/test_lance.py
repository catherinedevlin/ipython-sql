import base64
import io
import json

import pandas as pd
from PIL import Image
from sql.lance import ResultSet


def image_to_byte_array(image: Image) -> bytes:
    bytearr = io.BytesIO()
    image.save(bytearr, format=image.format)
    return bytearr.getvalue()


def _is_valid_image(img, img_bytes):
    return ((img["_lance_type"] == "image") and
            (img["data"] == base64.b64encode(img_bytes).decode("UTF-8")))


def test_image_embedded():
    with open("cat.jpg", "rb") as fh:
        img_bytes = image_to_byte_array(Image.open(fh))
    df = pd.DataFrame([{"image": None}, {"image": b""}, {"image": img_bytes}])
    rs = ResultSet(df, "foo")
    json_data = json.loads(rs.to_json())
    for record in json_data:
        img = record["image"]
        assert (img["data"] is None) or _is_valid_image(img, img_bytes)
