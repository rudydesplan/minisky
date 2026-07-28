#!/usr/bin/env python3

import json
import pathlib
import subprocess
import sys
import tempfile
import unittest


SANITIZER = pathlib.Path(__file__).with_name("phase12-sanitize-diagnostics.py")


class SanitizeDiagnosticsTest(unittest.TestCase):
    def run_sanitizer(
        self,
        source: pathlib.Path,
        destination: pathlib.Path,
        forbidden: dict[str, str],
        *,
        max_file: int,
        max_total: int,
    ) -> None:
        forbidden_file = source.parent / "test-forbidden.json"
        forbidden_file.write_text(json.dumps(forbidden))
        subprocess.run(
            [
                sys.executable,
                str(SANITIZER),
                "--source-dir",
                str(source),
                "--destination-dir",
                str(destination),
                "--forbidden-file",
                str(forbidden_file),
                "--max-file-bytes",
                str(max_file),
                "--max-total-bytes",
                str(max_total),
            ],
            check=True,
        )

    def test_redacts_before_file_truncation_with_multibyte_secret(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            source, destination = root / "source", root / "destination"
            source.mkdir()
            secret = "🔐秘密-token"
            encoded = secret.encode()
            limit = 64
            source.joinpath("minisky.log").write_bytes(
                b"A" * (limit - 15) + encoded + b"-safe-suffix"
            )

            self.run_sanitizer(
                source,
                destination,
                {"multibyte secret": secret},
                max_file=limit,
                max_total=limit,
            )

            output = destination.joinpath("minisky.log").read_bytes()
            self.assertLessEqual(len(output), limit)
            self.assertNotIn(encoded, output)
            self.assertNotIn(encoded[:2], output)

    def test_redacts_repeated_and_overlapping_values(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            source, destination = root / "source", root / "destination"
            source.mkdir()
            forbidden = {"long": "ababa", "left": "aba", "right": "bab", "repeat": "token"}
            source.joinpath("minisky.log").write_bytes(b"ababa token token abababa")

            self.run_sanitizer(
                source,
                destination,
                forbidden,
                max_file=128,
                max_total=128,
            )

            output = destination.joinpath("minisky.log").read_bytes()
            for value in forbidden.values():
                self.assertNotIn(value.encode(), output)

    def test_redacts_before_aggregate_cap_and_handles_binary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            source, destination = root / "source", root / "destination"
            source.mkdir()
            secret = b"aggregate-secret"
            source.joinpath("harness-summary.txt").write_bytes(b"\xff" * 29)
            source.joinpath("minisky.log").write_bytes(b"AAAA" + secret + b"\x00\xfe-safe")

            self.run_sanitizer(
                source,
                destination,
                {"aggregate": secret.decode()},
                max_file=32,
                max_total=48,
            )

            files = [path for path in destination.iterdir() if path.is_file()]
            self.assertLessEqual(sum(path.stat().st_size for path in files), 48)
            output = b"".join(path.read_bytes() for path in files)
            self.assertNotIn(secret, output)
            self.assertNotIn(secret[:2], output)

    def test_allows_only_safe_names_and_never_copies_raw_capture_material(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            source, destination = root / "source", root / "destination"
            source.mkdir()
            destination.mkdir()
            secret = "path-secret"
            source.joinpath("minisky.log").write_text(secret)
            source.joinpath("otlp-0001.pb").write_bytes(secret.encode())
            source.joinpath("forbidden-values.json").write_text(secret)
            source.joinpath("minisky.log.extra").write_text(secret)
            source.joinpath("nested").mkdir()
            source.joinpath("nested", "requests.json").write_text(secret)
            outside = root / "outside"
            outside.write_text("must-survive")
            source.joinpath("collector.log").symlink_to(outside)
            destination.joinpath("minisky.log").symlink_to(outside)
            destination.joinpath("stale-raw.bin").write_text(secret)

            self.run_sanitizer(
                source,
                destination,
                {"path": secret},
                max_file=128,
                max_total=256,
            )

            self.assertEqual(outside.read_text(), "must-survive")
            self.assertFalse(destination.joinpath("minisky.log").is_symlink())
            self.assertEqual(
                {path.name for path in destination.iterdir()},
                {"minisky.log"},
            )
            self.assertNotIn(secret.encode(), destination.joinpath("minisky.log").read_bytes())

    def test_rejects_symlink_destination_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            source, real_destination = root / "source", root / "real-destination"
            source.mkdir()
            real_destination.mkdir()
            source.joinpath("minisky.log").write_text("safe")
            destination = root / "destination"
            destination.symlink_to(real_destination, target_is_directory=True)
            forbidden_file = root / "forbidden.json"
            forbidden_file.write_text(json.dumps({"secret": "not-present"}))

            result = subprocess.run(
                [
                    sys.executable,
                    str(SANITIZER),
                    "--source-dir",
                    str(source),
                    "--destination-dir",
                    str(destination),
                    "--forbidden-file",
                    str(forbidden_file),
                    "--max-file-bytes",
                    "128",
                    "--max-total-bytes",
                    "256",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(list(real_destination.iterdir()), [])


if __name__ == "__main__":
    unittest.main()
