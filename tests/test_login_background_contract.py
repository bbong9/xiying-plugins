import json
import unittest
from pathlib import Path
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests/fixtures/login-background"
IMAGE_TYPES = {"SINGLE_POSTER", "SINGLE_IMAGE", "HERO_IMAGE"}


class LoginBackgroundContractTests(unittest.TestCase):
    def test_unified_background_replaces_the_two_active_background_entries(self):
        plugins = json.loads((ROOT / "plugins.json").read_text())
        plugins_by_id = {plugin["id"]: plugin for plugin in plugins}
        self.assertNotIn("org.xiying.bing-daily-background", plugins_by_id)
        self.assertNotIn("org.xiying.tmdb-trending-background", plugins_by_id)

        entry = plugins_by_id["org.xiying.login-background"]
        self.assertEqual(entry["binary"], "xiying-plugin-login-background")
        self.assertTrue(entry["version"])
        self.assertEqual(entry["manifest"], "manifests/org.xiying.login-background.json")
        manifest = json.loads((ROOT / entry["manifest"]).read_text())
        self.assertEqual(manifest["id"], "org.xiying.login-background")
        self.assertEqual(manifest["type"], "login_background")
        self.assertEqual(manifest["capabilities"], ["login_background.get"])
        fields = {field["key"]: field for field in manifest["configFields"]}
        self.assertEqual(
            [option["value"] for option in fields["source"]["options"]],
            ["BING_DAILY", "TMDB_TRENDING", "CUSTOM_IMAGE"],
        )
        for key in ("bingPersonalUseConfirmed", "tmdbLicenseConfirmed", "customImageRightsConfirmed"):
            self.assertEqual(fields[key]["type"], "toggle")
            self.assertFalse(fields[key]["defaultValue"])
        self.assertEqual(fields["customImage"]["type"], "image")
        self.assertEqual(manifest["permissions"]["filesystem"], [])

        for old_id, old_binary in (
            ("org.xiying.bing-daily-background", "xiying-plugin-bing-daily-background"),
            ("org.xiying.tmdb-trending-background", "xiying-plugin-tmdb-trending-background"),
        ):
            self.assertFalse((ROOT / f"manifests/{old_id}.json").exists())
            self.assertFalse((ROOT / f"src/bin/{old_binary}.rs").exists())

    def test_unified_migration_guide_requires_fresh_provider_consent(self):
        guide = (ROOT / "docs/login-background-migration.md").read_text()
        self.assertIn("org.xiying.login-background", guide)
        self.assertIn("重新逐项确认", guide)
        self.assertIn("不会迁移", guide)

    def test_wikimedia_provider_is_removed_from_the_active_store(self):
        plugin_id = "org.xiying.wikimedia-potd-background"
        plugins = json.loads((ROOT / "plugins.json").read_text())
        catalog = json.loads((ROOT / "index.json").read_text())

        self.assertNotIn(plugin_id, {plugin["id"] for plugin in plugins})
        self.assertNotIn(plugin_id, {plugin["id"] for plugin in catalog["plugins"]})
        self.assertFalse((ROOT / "manifests/org.xiying.wikimedia-potd-background.json").exists())
        self.assertFalse((ROOT / "src/bin/xiying-plugin-wikimedia-potd-background.rs").exists())

        workflow = (ROOT / ".github/workflows/login-background-validation.yml").read_text()
        self.assertNotIn(plugin_id, workflow)
        self.assertNotIn("xiying-plugin-wikimedia-potd-background", workflow)

    def test_provider_fixtures_match_manifest_hosts_and_bounded_response_contract(self):
        manifest = json.loads((FIXTURES / "manifest-v1.json").read_text())
        image_hosts = {host.lower() for host in manifest["permissions"]["imageHosts"]}
        network_hosts = {host.lower() for host in manifest["permissions"].get("network", [])}
        fixture_names = (
            "poster-feed-v1.json",
            "hero-image-v1.json",
            "single-poster-v1.json",
            "single-image-v1.json",
        )

        for fixture_name in fixture_names:
            with self.subTest(fixture=fixture_name):
                payload = json.loads((FIXTURES / fixture_name).read_text())
                self.assertLessEqual(
                    set(payload),
                    {"contentKind", "sourceName", "copyrightNotice", "items"},
                )
                self.assertIn(
                    payload["contentKind"],
                    {"POSTER_FEED", "HERO_IMAGE", "SINGLE_POSTER", "SINGLE_IMAGE"},
                )
                self.assertTrue(payload["sourceName"].strip())
                self.assertLessEqual(len(payload["items"]), 40)
                if payload["contentKind"] in IMAGE_TYPES:
                    self.assertEqual(len(payload["items"]), 1)
                self.assertLessEqual(len(json.dumps(payload).encode()), 256 * 1024)

                for item in payload["items"]:
                    self.assertLessEqual(
                        set(item),
                        {
                            "imageUrl",
                            "title",
                            "copyrightNotice",
                            "attributionUrl",
                            "licenseUrl",
                        },
                    )
                    self.assert_declared_https_url(item["imageUrl"], image_hosts)
                    for key in ("attributionUrl", "licenseUrl"):
                        if key in item:
                            self.assert_declared_https_url(item[key], network_hosts)

    def assert_declared_https_url(self, value, declared_hosts):
        parsed = urlsplit(value)
        self.assertEqual(parsed.scheme, "https")
        self.assertIn(parsed.hostname.lower(), declared_hosts)
        self.assertIsNone(parsed.username)
        self.assertIsNone(parsed.password)
        self.assertIsNone(parsed.port)
        self.assertFalse(parsed.fragment)


if __name__ == "__main__":
    unittest.main()
