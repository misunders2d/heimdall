"""Offline API-contract and employee-boundary tests; never use real credentials."""
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

spec = importlib.util.spec_from_file_location("employee_google", Path(__file__).with_name("google-workspace.py"))
google = importlib.util.module_from_spec(spec)
spec.loader.exec_module(google)

class EmployeeGoogleTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.root = Path(self.dir.name)
        self.access = self.root / "google-access.json"
        self.access.write_text(json.dumps({"email": "employee@example.test", "access_token": "SYNTHETIC_ACCESS", "expires": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(), "scopes": google.OAUTH_SCOPES}))
        env = {"HEIMDALL_GOOGLE_ACCESS_FILE": str(self.access), "HEIMDALL_TURN_DIR": str(self.root), "PI_EXTERNAL_MCP_PRIVATE": "1", "PI_EXTERNAL_PLATFORM": "slack"}
        self.env = patch.dict(os.environ, env, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_access_credentials_cannot_refresh_or_choose_another_account(self):
        creds = google.oauth_credentials({})
        self.assertEqual(creds.token, "SYNTHETIC_ACCESS")
        self.assertIsNone(creds.refresh_token)
        self.assertIsNone(creds.client_secret)
        self.assertIsNone(creds.client_id)
        for args in ({"email": "owner@example.test"}, {"subject": "owner@example.test"}, {"user_id": "other"}):
            with self.subTest(args=args), self.assertRaises(ValueError):
                google.oauth_credentials(args)

    def test_no_renewal_field_or_expired_snapshot_accepted(self):
        data = json.loads(self.access.read_text())
        for key in ("refresh_token", "client_secret", "client_id"):
            self.access.write_text(json.dumps(dict(data, **{key: "SYNTHETIC_FORBIDDEN"})))
            with self.assertRaises(ValueError):
                google.access_snapshot()
        data["expires"] = "2000-01-01T00:00:00Z"
        self.access.write_text(json.dumps(data))
        with self.assertRaises(ValueError):
            google.access_snapshot()

    def test_file_inputs_outputs_and_symlinks_cannot_escape_turn(self):
        local = self.root / "upload.txt"
        local.write_text("allowed")
        self.assertEqual(google.turn_path(str(local)), local)
        outside = self.root.parent / "not-this-turn"
        for path in (str(outside), str(self.access), "/root/.pi/agent/auth.json"):
            with self.subTest(path=path), self.assertRaises((ValueError, FileNotFoundError)):
                google.turn_path(path, writing=True)
        (self.root / "escape").symlink_to(self.root.parent, target_is_directory=True)
        with self.assertRaises(ValueError):
            google.turn_path(str(self.root / "escape" / "new.txt"), writing=True)

    def test_shared_guard_precedes_provider_and_suppresses_draft_metadata(self):
        os.environ["PI_EXTERNAL_MCP_PRIVATE"] = "0"
        for platform in ("slack", "telegram"):
            os.environ["PI_EXTERNAL_PLATFORM"] = platform
            for action in ("gmail_read", "gmail_search", "calendar_list", "calendar_events", "calendar_create_event", "calendar_update_event"):
                with self.subTest(platform=platform, action=action), patch.dict(google.ACTIONS, {action: MagicMock(side_effect=AssertionError("API must not be called"))}):
                    self.assertEqual(google.dispatch(action, {"confirm_write": True})["status"], "blocked")
        os.environ["PI_EXTERNAL_PLATFORM"] = "slack"
        draft = MagicMock(return_value={"status": "success", "draft": {"id": "PRIVATE_DRAFT"}})
        with patch.dict(google.ACTIONS, {"gmail_create_draft": draft}):
            result = google.dispatch("gmail_create_draft", {"confirm_write": True})
        self.assertNotIn("PRIVATE_DRAFT", json.dumps(result))
        self.assertFalse(result["sent"])

    def test_write_actions_require_confirmation_and_no_send_or_delete_exists(self):
        for action in google.WRITE_ACTIONS:
            with self.subTest(action=action), self.assertRaises(ValueError):
                google.dispatch(action, {})
        for action in ("gmail_send", "calendar_delete_event", "oauth_save_token", "oauth_disconnect"):
            self.assertNotIn(action, google.ACTIONS)

    def test_calendar_update_uses_selected_shared_calendar_and_patch(self):
        service = MagicMock()
        service.events.return_value.patch.return_value.execute.return_value = {"id": "event", "summary": "Updated"}
        with patch.object(google, "build", return_value=service) as build:
            data = google.dispatch("calendar_update_event", {"confirm_write": True, "calendar_id": "shared-calendar", "event_id": "event", "title": "Updated"})
        args = service.events.return_value.patch.call_args.kwargs
        self.assertEqual(args["calendarId"], "shared-calendar")
        self.assertEqual(args["eventId"], "event")
        self.assertEqual(args["body"], {"summary": "Updated"})
        self.assertEqual(data["event"]["id"], "event")
        self.assertIsNone(build.call_args.kwargs["credentials"].refresh_token)

    def test_calendar_create_formats_timezone_and_all_day_dates(self):
        service = MagicMock()
        service.events.return_value.insert.return_value.execute.return_value = {"id": "event"}
        with patch.object(google, "build", return_value=service):
            google.dispatch("calendar_create_event", {"confirm_write": True, "calendar_id": "shared", "title": "Event", "start_datetime": "2026-11-03", "end_datetime": "2026-11-04"})
        self.assertEqual(service.events.return_value.insert.call_args.kwargs["body"]["start"], {"date": "2026-11-03"})
        self.assertEqual(google._event_time("2026-11-03T09:00:00", "Europe/Berlin")["timeZone"], "Europe/Berlin")

    def test_sheets_read_write_append_and_tabs_use_caller_credentials(self):
        service = MagicMock()
        service.spreadsheets.return_value.get.return_value.execute.return_value = {"sheets": [{"properties": {"sheetId": 1, "title": "Tab"}}]}
        service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {"values": [[1]]}
        service.spreadsheets.return_value.values.return_value.update.return_value.execute.return_value = {"updatedCells": 1}
        service.spreadsheets.return_value.values.return_value.append.return_value.execute.return_value = {"updates": {"updatedCells": 1}}
        service.spreadsheets.return_value.batchUpdate.return_value.execute.return_value = {"replies": [{"addSheet": {"properties": {"sheetId": 2, "title": "New"}}}]}
        with patch.object(google, "build", return_value=service):
            for action in ("sheets_list_tabs", "sheets_read", "sheets_write", "sheets_append", "sheets_create_tab"):
                data = google.dispatch(action, {"spreadsheet_id": "a" * 24, "range": "Tab!A1", "values": [[1]], "title": "New", "confirm_write": True})
                self.assertEqual(data["status"], "success")
        self.assertEqual(service.spreadsheets.return_value.values.return_value.update.call_args.kwargs["body"], {"values": [[1]]})

    def test_drive_updates_require_app_created_marker(self):
        service = MagicMock()
        file = self.root / "file.txt"
        file.write_text("new content")
        service.files.return_value.get.return_value.execute.return_value = {"isAppAuthorized": True, "appProperties": {}}
        with patch.object(google, "build", return_value=service), self.assertRaises(ValueError):
            google.dispatch("drive_update_file", {"confirm_write": True, "file_id": "a" * 24, "local_path": str(file)})
        service.files.return_value.update.assert_not_called()
        service.files.return_value.get.return_value.execute.return_value = {"isAppAuthorized": True, "appProperties": {"heimdallEmployeeCreated": "v1"}}
        service.files.return_value.update.return_value.execute.return_value = {"id": "a" * 24}
        with patch.object(google, "build", return_value=service):
            data = google.dispatch("drive_update_file", {"confirm_write": True, "file_id": "a" * 24, "local_path": str(file)})
        self.assertEqual(data["status"], "success")

    def test_gmail_creates_real_draft_without_sending(self):
        service = MagicMock()
        service.users.return_value.drafts.return_value.create.return_value.execute.return_value = {"id": "draft", "message": {"id": "message"}}
        with patch.object(google, "build", return_value=service):
            data = google.dispatch("gmail_create_draft", {"confirm_write": True, "to": "to@example.test", "email_subject": "Subject", "body": "Body"})
        self.assertFalse(data["sent"])
        self.assertEqual(data["draft"]["id"], "draft")
        self.assertEqual(service.users.return_value.drafts.return_value.create.call_args.kwargs["userId"], "me")
        service.users.return_value.messages.return_value.send.assert_not_called()

if __name__ == "__main__":
    unittest.main()
