#!/usr/bin/env python3
"""Generate the brand icons HACS and the Home Assistant brands repo expect.

Kept as a script rather than as two opaque binaries so the artwork is
reproducible and reviewable in a diff. Regenerate with:

    python3 scripts/make-brand-assets.py

The motif is a transit line diagram: a vertical route with two stops on it.
Chosen because it stays legible at 24 pixels, which is the size that actually
matters -- an icon is read in a sidebar, not on a landing page.
"""

from __future__ import annotations

import pathlib

from PIL import Image, ImageDraw

OUT = pathlib.Path(__file__).resolve().parent.parent / "custom_components" / "gtfsie" / "brand"

#: Home Assistant's own blue, so the icon sits with the rest of the sidebar
#: rather than competing with it.
INK = (3, 169, 244, 255)
PAPER = (255, 255, 255, 255)


def draw(size: int) -> Image.Image:
    """One square icon at the given edge length."""
    # Drawn at 4x and downsampled: PIL has no antialiasing on shapes, and a
    # hard-edged circle at 256 pixels looks obviously machine-made.
    scale = 4
    edge = size * scale
    image = Image.new("RGBA", (edge, edge), (0, 0, 0, 0))
    pen = ImageDraw.Draw(image)

    radius = int(edge * 0.22)
    pen.rounded_rectangle([0, 0, edge - 1, edge - 1], radius=radius, fill=INK)

    # The route: a vertical bar with a stop near each end.
    bar_w = int(edge * 0.085)
    x = edge // 2
    top, bottom = int(edge * 0.26), int(edge * 0.74)
    pen.line([(x, top), (x, bottom)], fill=PAPER, width=bar_w)

    stop_r = int(edge * 0.105)
    for y in (top, bottom):
        pen.ellipse([x - stop_r, y - stop_r, x + stop_r, y + stop_r], fill=PAPER)
        inner = int(stop_r * 0.45)
        pen.ellipse([x - inner, y - inner, x + inner, y + inner], fill=INK)

    return image.resize((size, size), Image.LANCZOS)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, size in (("icon.png", 256), ("icon@2x.png", 512)):
        draw(size).save(OUT / name, optimize=True)
        print(f"wrote {OUT / name} ({size}x{size})")


if __name__ == "__main__":
    main()
