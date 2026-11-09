"""`uv run setup-rtce` — Codex config.toml merge, and Claude Code / Copilot registration.

Nothing here touches the developer's real ``~/.codex/config.toml``: every test
points ``write_codex_config`` at a path inside a temp directory.
"""

import io
import json
import subprocess
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from scripts import setup_rtce
from scripts.setup_rtce import basic_token, warn_on_empty_card_fields, write_codex_config

ENDPOINT = "https://mcp.us-east-1.aws.confluent.cloud/mcp/v1/context-engine/organizations/org/environments/env/kafka-clusters/lkc"
TOKEN = basic_token("KEY", "SECRET")


class WriteCodexConfigTests(unittest.TestCase):
    def test_creates_file_when_missing(self):
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / ".codex" / "config.toml"
            self.assertFalse(config_path.exists())

            with redirect_stdout(io.StringIO()):
                ok = write_codex_config(ENDPOINT, TOKEN, config_path)

            self.assertTrue(ok)
            text = config_path.read_text()
            self.assertIn("[mcp_servers.real-time-context-engine]", text)
            self.assertIn(ENDPOINT, text)
            self.assertIn(f"Basic {TOKEN}", text)

    def test_preserves_unrelated_existing_content(self):
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text('# a comment worth keeping\n[mcp_servers.other]\ncommand = "some-other-server"\n')

            with redirect_stdout(io.StringIO()):
                write_codex_config(ENDPOINT, TOKEN, config_path)

            text = config_path.read_text()
            self.assertIn("# a comment worth keeping", text)
            self.assertIn("[mcp_servers.other]", text)
            self.assertIn("some-other-server", text)
            self.assertIn("[mcp_servers.real-time-context-engine]", text)

    def test_rerun_replaces_existing_rtce_entry(self):
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"

            with redirect_stdout(io.StringIO()):
                write_codex_config("https://old-endpoint", basic_token("OLD", "SECRET"), config_path)
                write_codex_config(ENDPOINT, TOKEN, config_path)

            text = config_path.read_text()
            self.assertNotIn("old-endpoint", text)
            self.assertIn(ENDPOINT, text)
            self.assertEqual(text.count("[mcp_servers.real-time-context-engine]"), 1)

    def test_dry_run_does_not_write(self):
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / ".codex" / "config.toml"

            with redirect_stdout(io.StringIO()) as out:
                ok = write_codex_config(ENDPOINT, TOKEN, config_path, dry_run=True)

            self.assertTrue(ok)
            self.assertFalse(config_path.exists())
            self.assertIn("dry-run", out.getvalue())

    def test_fails_without_writing_on_unparseable_existing_file(self):
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.toml"
            config_path.write_text("this is not [ valid toml at all\n===\n")

            with redirect_stdout(io.StringIO()) as out:
                ok = write_codex_config(ENDPOINT, TOKEN, config_path)

            self.assertFalse(ok)
            self.assertIn("couldn't auto-edit", out.getvalue())
            # the broken file is left untouched, not overwritten
            self.assertIn("not [ valid toml", config_path.read_text())


def _done(returncode=0, stderr=""):
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout="", stderr=stderr)


class CopilotCliTests(unittest.TestCase):
    def test_add_sends_the_basic_token_as_a_header_over_http(self):
        remove_argv, add_argv = setup_rtce.copilot_cli_argv(ENDPOINT, TOKEN)
        self.assertEqual(remove_argv, ["copilot", "mcp", "remove", "real-time-context-engine"])
        self.assertEqual(add_argv[:5], ["copilot", "mcp", "add", "--transport", "http"])
        self.assertIn(f"Authorization: Basic {TOKEN}", add_argv)
        self.assertEqual(add_argv[-2:], ["real-time-context-engine", ENDPOINT])

    def test_registration_removes_then_adds(self):
        with patch.object(setup_rtce, "_run", return_value=_done()) as run, redirect_stdout(io.StringIO()):
            ok = setup_rtce.register_copilot_cli(ENDPOINT, TOKEN)
        self.assertTrue(ok)
        self.assertEqual(
            [c.args[0][:3] for c in run.call_args_list], [["copilot", "mcp", "remove"], ["copilot", "mcp", "add"]]
        )

    def test_a_refused_registration_fails(self):
        with (
            patch.object(setup_rtce, "_run", return_value=_done(1, "bad header")),
            redirect_stdout(io.StringIO()) as out,
        ):
            ok = setup_rtce.register_copilot_cli(ENDPOINT, TOKEN)
        self.assertFalse(ok)
        self.assertIn("bad header", out.getvalue())

    def test_dry_run_runs_nothing(self):
        with patch.object(setup_rtce, "_run", side_effect=AssertionError("ran")), redirect_stdout(io.StringIO()) as out:
            self.assertTrue(setup_rtce.register_copilot_cli(ENDPOINT, TOKEN, dry_run=True))
        self.assertIn("copilot mcp add", out.getvalue())


class VSCodeTests(unittest.TestCase):
    def test_add_mcp_payload_is_a_named_http_server_with_the_basic_header(self):
        argv = setup_rtce.vscode_argv(ENDPOINT, TOKEN)
        self.assertEqual(argv[:2], ["code", "--add-mcp"])
        self.assertEqual(
            json.loads(argv[2]),
            {
                "name": "real-time-context-engine",
                "type": "http",
                "url": ENDPOINT,
                "headers": {"Authorization": f"Basic {TOKEN}"},
            },
        )

    def test_missing_code_command_fails(self):
        with patch.object(setup_rtce, "_run", side_effect=FileNotFoundError), redirect_stdout(io.StringIO()) as out:
            ok = setup_rtce.register_vscode(ENDPOINT, TOKEN)
        self.assertFalse(ok)
        self.assertIn("'code' not found", out.getvalue())


class ManualInstructionsTests(unittest.TestCase):
    def test_every_agent_gets_steps_carrying_the_endpoint_and_token(self):
        for client in ("claude", "codex", "vscode", "copilot"):
            with self.subTest(client=client):
                text = setup_rtce.manual_instructions(client, ENDPOINT, TOKEN)
                self.assertIn(ENDPOINT, text)
                self.assertIn(f"Basic {TOKEN}", text)

    def test_json_steps_paste_as_valid_config(self):
        vscode = setup_rtce.manual_instructions("vscode", ENDPOINT, TOKEN)
        config = json.loads(vscode[vscode.index("{") :])
        self.assertEqual(config["servers"]["real-time-context-engine"]["type"], "http")

        copilot = setup_rtce.manual_instructions("copilot", ENDPOINT, TOKEN)
        config = json.loads(copilot[copilot.index("{") :])
        self.assertEqual(config["mcpServers"]["real-time-context-engine"]["tools"], ["*"])

    def test_printed_after_success_and_after_failure(self):
        card = {"F1_RTCE_MCP_ENDPOINT": ENDPOINT, "F1_RTCE_API_KEY": "KEY", "F1_RTCE_API_SECRET": "SECRET"}
        for outcome in (True, False):
            with self.subTest(registered=outcome):
                with (
                    patch.object(setup_rtce.sys, "argv", ["setup-rtce", "--client", "vscode"]),
                    patch.object(setup_rtce, "load_card", return_value=(Path("card.env"), card)),
                    patch.object(setup_rtce, "get_project_root", return_value=Path(".")),
                    patch.object(setup_rtce, "resolve_rtce_credentials", side_effect=lambda c, *a, **k: c),
                    patch.object(setup_rtce, "register_vscode", return_value=outcome),
                    redirect_stdout(io.StringIO()) as out,
                ):
                    if outcome:
                        setup_rtce.main()
                    else:
                        with self.assertRaises(SystemExit):
                            setup_rtce.main()
                self.assertIn("add the server by hand", out.getvalue())
                self.assertIn("MCP: Open User Configuration", out.getvalue())


class ClientChoiceTests(unittest.TestCase):
    def test_choices(self):
        cases = {
            "": ["claude"],
            "1": ["claude"],
            "3": ["vscode"],
            "4": ["copilot"],
            "1,3": ["claude", "vscode"],
            "copilot claude": ["claude", "copilot"],
            "both": ["claude", "codex"],
            "5": ["claude", "codex", "vscode", "copilot"],
            "all": ["claude", "codex", "vscode", "copilot"],
            "9": None,
            "1,teams": None,
        }
        for choice, expected in cases.items():
            with self.subTest(choice=choice):
                self.assertEqual(setup_rtce._parse_client_choice(choice), expected)


class EmptyCardHintTests(unittest.TestCase):
    def test_regenerate_hint_names_every_card_writing_deploy_track(self):
        with redirect_stdout(io.StringIO()) as out:
            missing = warn_on_empty_card_fields({})
        self.assertIn("F1_RTCE_API_KEY", missing)
        self.assertIn("uv run deploy --track self-service", out.getvalue())
        self.assertIn("uv run deploy --track pre-provisioned", out.getvalue())


PREPROVISIONED_CARD = {
    "F1_ENVIRONMENT_ID": "env-mine",
    "F1_CLUSTER_ID": "lkc-mine",
    "F1_RTCE_API_KEY": "GLOBALKEY",
    "F1_RTCE_API_SECRET": "GLOBALSECRET",
}


class PreprovisionedCardTests(unittest.TestCase):
    """The pre-provisioned track runs no Terraform; its card carries the developer's Global key."""

    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        root = Path(self._tmp.name)
        # Left behind by the old developer Terraform root: never read again.
        state = root / "terraform" / "pre-provisioned" / "terraform.tfstate"
        state.parent.mkdir(parents=True)
        state.write_text("{}")
        for p in (
            patch.object(setup_rtce, "get_project_root", return_value=root),
            patch.object(setup_rtce.subprocess, "run", side_effect=AssertionError("terraform called")),
            patch("builtins.input", side_effect=AssertionError("prompted")),
        ):
            p.start()
            self.addCleanup(p.stop)

    def test_no_terraform_state_is_scanned_for_it(self):
        self.assertEqual(setup_rtce.terraform_rtce_outputs(PREPROVISIONED_CARD), {})

    def test_the_cards_global_key_is_used_as_is(self):
        resolved = setup_rtce.resolve_rtce_credentials(PREPROVISIONED_CARD)
        self.assertEqual((resolved["F1_RTCE_API_KEY"], resolved["F1_RTCE_API_SECRET"]), ("GLOBALKEY", "GLOBALSECRET"))


if __name__ == "__main__":
    unittest.main()
