"""Crop /pr-uat terminal captures to their content, dropping the empty rows below the last line of output.

Usage: uv run --with pillow python .claude/skills/pr-uat/crop-terminal.py <image> [<image> ...]

PIL (not a Morpheus dependency, hence `--with pillow`) because ImageMagick isn't installed here, and
`sips -c` crops from the centre, which cuts the command line off the top of the capture.
"""

import sys

from PIL import Image, ImageChops

PADDING = 16
# JPEG noise makes an empty terminal differ from its background by a few levels; text differs by far more.
THRESHOLD = 40


def crop(path: str) -> None:
    image = Image.open(path)
    rgb = image.convert('RGB')
    background = Image.new('RGB', rgb.size, rgb.getpixel((rgb.width - 1, rgb.height - 1)))
    content = ImageChops.difference(rgb, background).convert('L').point(lambda level: 255 if level > THRESHOLD else 0)
    box = content.getbbox()
    if box is None:
        print(f'{path}: blank capture, left as is — retake it')
        return
    bottom = min(image.height, box[3] + PADDING)
    # Pillow re-encodes a JPEG at quality 75 by default, which blurs terminal text.
    image.crop((0, 0, image.width, bottom)).save(path, quality=95)
    print(f'{path}: {image.height}px -> {bottom}px')


if __name__ == '__main__':
    for image_path in sys.argv[1:]:
        crop(image_path)
