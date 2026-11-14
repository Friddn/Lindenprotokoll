import tempfile
import unittest
import shutil
from pathlib import Path

from PIL import Image, ImageDraw

from lindenprotokoll.app.graph_digitizer import digitize


class GraphDigitizerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.image_path = Path(self.temp.name) / "graph.png"
        image = Image.new("RGB", (800, 400), "white")
        draw = ImageDraw.Draw(image)
        draw.line(
            [(50, 300), (180, 260), (300, 260), (420, 200), (560, 200), (740, 250)],
            fill=(0, 180, 60), width=4,
        )
        draw.line(
            [(50, 350), (200, 300), (350, 310), (500, 240), (740, 270)],
            fill=(210, 40, 40), width=4,
        )
        image.save(self.image_path)

    def tearDown(self):
        self.temp.cleanup()

    def test_green_and_red_curves_are_detected(self):
        for curve in ("green", "red"):
            points, cadence = digitize(
                str(self.image_path), "2026-11-12", 50, 740, 50, 370, 0, 10, curve
            )
            self.assertGreaterEqual(len(points), 3)
            self.assertTrue(all(0 <= p["value_kw"] <= 10 for p in points))
            self.assertTrue(all(p["confidence"] < 1 for p in points))
            self.assertTrue(all("point_kind" in p for p in points))
            # Cadence is intentionally unknown unless repeated spacing is found.
            self.assertTrue(cadence is None or cadence > 0)


    def test_shaded_area_does_not_pull_red_curve_toward_zero(self):
        # A red line chart commonly shades the area below its curve. The trace
        # must follow the saturated stroke, not the median of line + fill.
        image = Image.new("RGB", (400, 240), "white")
        draw = ImageDraw.Draw(image)
        draw.polygon([(20, 180), (80, 60), (160, 110), (240, 40), (380, 150), (380, 180)], fill=(246, 198, 186))
        draw.line([(20, 180), (80, 60), (160, 110), (240, 40), (380, 150)], fill=(235, 79, 47), width=5)
        image.save(self.image_path)
        points, _ = digitize(str(self.image_path), "2026-11-12", 20, 380, 20, 200, 0, 10, "red")
        self.assertGreater(max(p["value_kw"] for p in points), 8.0)

    @unittest.skipUnless(shutil.which("tesseract") and Path("/mnt/data/IMG_9140.png").exists(), "real screenshot/Tesseract unavailable")
    def test_real_screenshot_auto_calibration_and_curve_peaks(self):
        from lindenprotokoll.app.graph_digitizer import auto_calibrate
        screenshot = "/mnt/data/IMG_9140.png"
        peaks = {}
        for curve in ("green", "red"):
            cal = auto_calibrate(screenshot, curve)
            points, _ = digitize(screenshot, "2026-11-06", cal["x_left"], cal["x_right"], cal["y_top"], cal["y_bottom"], cal["y_min"], cal["y_max"], curve)
            peaks[curve] = max(p["value_kw"] for p in points)
        self.assertGreater(peaks["green"], 2.5)
        self.assertLess(peaks["green"], 3.6)
        self.assertGreater(peaks["red"], 7.5)
        self.assertLess(peaks["red"], 9.5)

    def test_invalid_axis_range_is_rejected(self):
        with self.assertRaises(ValueError):
            digitize(str(self.image_path), "2026-11-12", 50, 740, 50, 370, 10, 0, "green")

    def test_invalid_date_is_rejected(self):
        with self.assertRaises(ValueError):
            digitize(str(self.image_path), "not-a-date", 50, 740, 50, 370, 0, 10, "green")


if __name__ == "__main__":
    unittest.main()
