#!/usr/bin/env python3
"""Employee access-only port of hub google_workspace_sa.py (3e28082).
No OAuth enrollment, renewal, database or service account exists in this helper.
"""

from __future__ import annotations

import base64
from email.message import EmailMessage
import io
import json
import os
import re
import sys
import time
import tempfile
from pathlib import Path
from typing import Any

from google.oauth2.credentials import Credentials as UserCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/calendar.events",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/userinfo.email",
]

DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"
SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
CALENDAR_RO_SCOPE = "https://www.googleapis.com/auth/calendar.readonly"
CALENDAR_EVENTS_SCOPE = "https://www.googleapis.com/auth/calendar.events"
GMAIL_RO_SCOPE = "https://www.googleapis.com/auth/gmail.readonly"
GMAIL_COMPOSE_SCOPE = "https://www.googleapis.com/auth/gmail.compose"

ID_RE = re.compile(
    r"(?:/(?:spreadsheets|document|presentation|forms|drawings|file)/d/|/folders/|[?&]id=)([A-Za-z0-9_-]{20,100})"
)
BARE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{20,100}$")

GOOGLE_EXPORTS = {
    "application/vnd.google-apps.document": "text/plain",
    "application/vnd.google-apps.spreadsheet": "text/csv",
    "application/vnd.google-apps.presentation": "text/plain",
    "application/vnd.google-apps.drawing": "image/png",
}

TEXT_MIME_PREFIXES = ("text/",)
TEXT_MIME_EXACT = {
    "application/json",
    "application/xml",
    "application/javascript",
    "application/x-yaml",
    "application/vnd.google-apps.script+json",
}


def out(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False))


def fail(message: str, **extra: Any) -> None:
    payload = {"status": "error", "message": message}
    payload.update(extra)
    out(payload)


def extract_id(url_or_id: str) -> str:
    s = (url_or_id or "").strip()
    if BARE_ID_RE.fullmatch(s):
        return s
    m = ID_RE.search(s)
    if m:
        return m.group(1)
    raise ValueError("expected a Google Drive URL or file/folder ID")


def requested_subject(args: dict[str, Any]) -> None:
    if args.get("subject") or args.get("user_id"):
        raise ValueError("Google identity cannot be selected by tool arguments")


def turn_path(value: str, writing: bool = False) -> Path:
    root = Path(os.environ["HEIMDALL_TURN_DIR"]).resolve(strict=True)
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = root / path
    path = path.resolve(strict=not writing)
    if not path.is_relative_to(root) or any(p in {"auth.json", "mcp-auth.json", "google-access.json", "settings.json", "mcp.json"} for p in path.parts):
        raise ValueError("Google file access is confined to this turn")
    return path


def access_snapshot() -> dict[str, Any]:
    # Fixed by the trusted launcher, never accepted from tool arguments.
    path = Path(os.environ["HEIMDALL_GOOGLE_ACCESS_FILE"])
    snapshot = json.loads(path.read_text())
    if set(snapshot) - {"email", "access_token", "expires", "scopes"}:
        raise ValueError("Invalid access-only Google snapshot")
    from datetime import datetime, timezone
    if not snapshot.get("access_token") or datetime.fromisoformat(snapshot["expires"].replace("Z", "+00:00")) <= datetime.now(timezone.utc):
        raise ValueError("Send `google login` in a DM.")
    return snapshot


def resolve_oauth_email(args: dict[str, Any]) -> str:
    requested_subject(args)
    snapshot = access_snapshot()
    if args.get("email") and args["email"].strip().lower() != snapshot["email"]:
        raise ValueError("Requested Google account is not connected to this caller")
    return snapshot["email"]


def oauth_credentials(args: dict[str, Any]) -> UserCredentials:
    resolve_oauth_email(args)
    snapshot = access_snapshot()
    # No token_uri, refresh token, client secret, ADC or service-account fallback.
    return UserCredentials(token=snapshot["access_token"], scopes=snapshot["scopes"])


def safe_http_error(e: HttpError) -> dict[str, Any]:
    status = getattr(e.resp, "status", None)
    reason = getattr(e.resp, "reason", "") or ""
    msg = reason
    try:
        data = json.loads(e.content.decode("utf-8", errors="replace"))
        err = data.get("error", {})
        msg = err.get("message") or err.get("status") or msg
    except Exception:
        pass
    return {"status": "error", "http_status": status, "message": "Google request failed; check your permissions or send `google login` in a DM."}


def drive_service(args: dict[str, Any]):
    requested_subject(args)
    return build("drive", "v3", credentials=oauth_credentials(args), cache_discovery=False)


def drive_list(args: dict[str, Any]) -> dict[str, Any]:
    service = drive_service(args)
    q_parts = []
    query = (args.get("query") or "").strip()
    folder = (args.get("folder_id") or "").strip()
    if query:
        q_parts.append(query)
    if folder:
        q_parts.append(f"'{extract_id(folder)}' in parents")
    q_parts.append("trashed = false")
    res = service.files().list(
        q=" and ".join(q_parts),
        pageSize=min(int(args.get("max_results") or 20), 100),
        fields="files(id,name,mimeType,modifiedTime,size,webViewLink,parents)",
        orderBy="modifiedTime desc",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    return {"status": "success", "count": len(res.get("files", [])), "files": res.get("files", [])}


def drive_metadata(args: dict[str, Any]) -> dict[str, Any]:
    service = drive_service(args)
    fid = extract_id(args.get("file_id") or "")
    meta = service.files().get(
        fileId=fid,
        fields="id,name,mimeType,modifiedTime,size,webViewLink,parents,owners(displayName,emailAddress)",
        supportsAllDrives=True,
    ).execute()
    return {"status": "success", "file": meta}


def is_text_mime(mime: str) -> bool:
    m = (mime or "").split(";", 1)[0].lower()
    return m.startswith(TEXT_MIME_PREFIXES) or m in TEXT_MIME_EXACT


def drive_download(args: dict[str, Any]) -> dict[str, Any]:
    service = drive_service(args)
    fid = extract_id(args.get("file_id") or "")
    meta = service.files().get(fileId=fid, fields="id,name,mimeType,size", supportsAllDrives=True).execute()
    mime = meta.get("mimeType", "")
    export_mime = args.get("export_mime") or GOOGLE_EXPORTS.get(mime)
    if mime.startswith("application/vnd.google-apps."):
        if not export_mime:
            return {"status": "error", "message": f"No default export MIME for {mime}; pass export_mime."}
        request = service.files().export_media(fileId=fid, mimeType=export_mime)
        output_mime = export_mime
    else:
        request = service.files().get_media(fileId=fid, supportsAllDrives=True)
        output_mime = mime

    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
        if buf.tell() > 20 * 1024 * 1024:
            raise ValueError("Google download exceeds the 20 MiB limit")
    data = buf.getvalue()

    save_path = (args.get("save_path") or "").strip()
    if save_path:
        p = turn_path(save_path, writing=True)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        return {"status": "success", "file": meta, "mimeType": output_mime, "saved_to": str(p), "bytes": len(data)}

    if is_text_mime(output_mime):
        return {"status": "success", "file": meta, "mimeType": output_mime, "text": data.decode("utf-8", errors="replace"), "bytes": len(data)}

    return {
        "status": "success",
        "file": meta,
        "mimeType": output_mime,
        "bytes": len(data),
        "base64": base64.b64encode(data).decode("ascii") if len(data) <= 2_000_000 else None,
        "message": "Binary content returned as base64 only when <=2MB. Use save_path for larger files.",
    }


def drive_upload_text(args: dict[str, Any]) -> dict[str, Any]:
    service = drive_service(args)
    name = args.get("name") or "untitled.txt"
    content = args.get("content") or ""
    mime = args.get("mime_type") or "text/plain"
    folder = (args.get("folder_id") or "").strip()
    fd, name_tmp = tempfile.mkstemp(prefix="google-upload-", dir=os.environ["HEIMDALL_TURN_DIR"])
    tmp = Path(name_tmp)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        stream.write(content)
    try:
        body: dict[str, Any] = {"name": name, "appProperties": {"heimdallEmployeeCreated": "v1"}}
        if folder:
            body["parents"] = [extract_id(folder)]
        media = MediaFileUpload(str(tmp), mimetype=mime, resumable=False)
        created = service.files().create(
            body=body,
            media_body=media,
            fields="id,name,mimeType,webViewLink",
            supportsAllDrives=True,
        ).execute()
        return {"status": "success", "file": created}
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def drive_upload_file(args: dict[str, Any]) -> dict[str, Any]:
    service = drive_service(args)
    local_path = turn_path(args.get("local_path") or "")
    if not local_path.is_file():
        return {"status": "error", "message": "local_path is not a file"}
    name = args.get("name") or local_path.name
    mime = args.get("mime_type") or None
    folder = (args.get("folder_id") or "").strip()
    body: dict[str, Any] = {"name": name, "appProperties": {"heimdallEmployeeCreated": "v1"}}
    if folder:
        body["parents"] = [extract_id(folder)]
    media = MediaFileUpload(str(local_path), mimetype=mime, resumable=True)
    created = service.files().create(
        body=body,
        media_body=media,
        fields="id,name,mimeType,webViewLink",
        supportsAllDrives=True,
    ).execute()
    return {"status": "success", "file": created}


def drive_update_file(args: dict[str, Any]) -> dict[str, Any]:
    service = drive_service(args)
    fid = extract_id(args.get("file_id") or "")
    meta = service.files().get(fileId=fid, fields="isAppAuthorized,appProperties", supportsAllDrives=True).execute()
    if not meta.get("isAppAuthorized") or meta.get("appProperties", {}).get("heimdallEmployeeCreated") != "v1":
        raise ValueError("Only files created by the employee Google tools can be updated")

    local_path = turn_path(args.get("local_path") or "")
    if not local_path.is_file():
        return {"status": "error", "message": "local_path is not a file"}
    mime = args.get("mime_type") or None
    body: dict[str, Any] = {}
    if args.get("name"):
        body["name"] = args["name"]
    media = MediaFileUpload(str(local_path), mimetype=mime, resumable=True)
    updated = service.files().update(
        fileId=fid,
        body=body,
        media_body=media,
        fields="id,name,mimeType,webViewLink,modifiedTime",
        supportsAllDrives=True,
    ).execute()
    return {"status": "success", "file": updated}


def drive_create_folder(args: dict[str, Any]) -> dict[str, Any]:
    service = drive_service(args)
    name = args.get("name") or "New Folder"
    folder = (args.get("parent_folder_id") or "").strip()
    body: dict[str, Any] = {"name": name, "mimeType": "application/vnd.google-apps.folder", "appProperties": {"heimdallEmployeeCreated": "v1"}}
    if folder:
        body["parents"] = [extract_id(folder)]
    created = service.files().create(body=body, fields="id,name,webViewLink", supportsAllDrives=True).execute()
    return {"status": "success", "folder": created}


def sheets_service(args: dict[str, Any]):
    requested_subject(args)
    return build("sheets", "v4", credentials=oauth_credentials(args), cache_discovery=False)


def parse_values(values: Any) -> list[list[Any]]:
    if values is None:
        return []
    if isinstance(values, str):
        values = json.loads(values)
    if not isinstance(values, list):
        raise ValueError("values must be a 2D array")
    if values and not isinstance(values[0], list):
        values = [values]
    return values


def sheets_list_tabs(args: dict[str, Any]) -> dict[str, Any]:
    service = sheets_service(args)
    sid = extract_id(args.get("spreadsheet_id") or "")
    meta = service.spreadsheets().get(spreadsheetId=sid, fields="spreadsheetId,spreadsheetUrl,properties(title),sheets(properties(sheetId,title,index,gridProperties(rowCount,columnCount)))").execute()
    tabs = []
    for s in meta.get("sheets", []):
        p = s.get("properties", {})
        tabs.append({"sheetId": p.get("sheetId"), "title": p.get("title"), "index": p.get("index"), "rows": p.get("gridProperties", {}).get("rowCount"), "columns": p.get("gridProperties", {}).get("columnCount")})
    return {"status": "success", "spreadsheet": {"id": meta.get("spreadsheetId"), "title": meta.get("properties", {}).get("title"), "url": meta.get("spreadsheetUrl")}, "tabs": tabs}


def sheets_read(args: dict[str, Any]) -> dict[str, Any]:
    service = sheets_service(args)
    sid = extract_id(args.get("spreadsheet_id") or "")
    range_ = args.get("range") or "A1:Z1000"
    res = service.spreadsheets().values().get(spreadsheetId=sid, range=range_).execute()
    values = res.get("values", [])
    return {"status": "success", "range": res.get("range"), "rows": len(values), "values": values}


def sheets_append(args: dict[str, Any]) -> dict[str, Any]:
    service = sheets_service(args)
    sid = extract_id(args.get("spreadsheet_id") or "")
    range_ = args.get("range") or "A1"
    values = parse_values(args.get("values"))
    if not values:
        return {"status": "error", "message": "values required"}
    res = service.spreadsheets().values().append(
        spreadsheetId=sid,
        range=range_,
        valueInputOption=args.get("value_input_option") or "USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute()
    return {"status": "success", "updates": res.get("updates", {}), "spreadsheetId": sid}


def sheets_write(args: dict[str, Any]) -> dict[str, Any]:
    service = sheets_service(args)
    sid = extract_id(args.get("spreadsheet_id") or "")
    range_ = args.get("range") or "A1"
    values = parse_values(args.get("values"))
    res = service.spreadsheets().values().update(
        spreadsheetId=sid,
        range=range_,
        valueInputOption=args.get("value_input_option") or "USER_ENTERED",
        body={"values": values},
    ).execute()
    return {"status": "success", "updatedRows": res.get("updatedRows"), "updatedCells": res.get("updatedCells"), "updatedRange": res.get("updatedRange"), "spreadsheetId": sid}


def sheets_create_tab(args: dict[str, Any]) -> dict[str, Any]:
    service = sheets_service(args)
    sid = extract_id(args.get("spreadsheet_id") or "")
    title = (args.get("title") or "Pi Report")[:100]
    res = service.spreadsheets().batchUpdate(
        spreadsheetId=sid,
        body={"requests": [{"addSheet": {"properties": {"title": title}}}]},
    ).execute()
    sheet = res.get("replies", [{}])[0].get("addSheet", {}).get("properties", {})
    return {"status": "success", "spreadsheetId": sid, "sheet": {"sheetId": sheet.get("sheetId"), "title": sheet.get("title"), "index": sheet.get("index")}}


def calendar_service(args: dict[str, Any]):
    requested_subject(args)
    return build("calendar", "v3", credentials=oauth_credentials(args), cache_discovery=False)


def calendar_list(args: dict[str, Any]) -> dict[str, Any]:
    service = calendar_service(args)
    res = service.calendarList().list(minAccessRole="reader", maxResults=min(int(args.get("max_results") or 50), 250)).execute()
    items = [
        {
            "id": c.get("id"),
            "summary": c.get("summary"),
            "primary": c.get("primary", False),
            "accessRole": c.get("accessRole"),
            "timeZone": c.get("timeZone"),
        }
        for c in res.get("items", [])
    ]
    return {"status": "success", "count": len(items), "calendars": items}


def require_oauth_scope(args: dict[str, Any], scope: str) -> None:
    resolve_oauth_email(args)
    if scope not in access_snapshot()["scopes"]:
        raise ValueError("Google scope missing. Send `google login` in a DM.")


def calendar_events(args: dict[str, Any]) -> dict[str, Any]:
    service = calendar_service(args)
    calendar_id = args.get("calendar_id") or "primary"
    max_results = min(int(args.get("max_results") or 25), 250)
    params: dict[str, Any] = {
        "calendarId": calendar_id,
        "maxResults": max_results,
        "singleEvents": True,
        "orderBy": "startTime",
    }
    if args.get("time_min"):
        params["timeMin"] = args["time_min"]
    if args.get("time_max"):
        params["timeMax"] = args["time_max"]
    if args.get("query"):
        params["q"] = args["query"]
    res = service.events().list(**params).execute()
    events = [
        {
            "id": e.get("id"),
            "summary": e.get("summary", "(no title)"),
            "start": e.get("start", {}).get("dateTime") or e.get("start", {}).get("date"),
            "end": e.get("end", {}).get("dateTime") or e.get("end", {}).get("date"),
            "location": e.get("location"),
            "description": (e.get("description") or "")[:500],
            "htmlLink": e.get("htmlLink"),
            "status": e.get("status"),
        }
        for e in res.get("items", [])
    ]
    return {"status": "success", "count": len(events), "events": events}


def _event_time(value: str, timezone: str | None = None) -> dict[str, Any]:
    v = (value or "").strip()
    if not v:
        raise ValueError("event start/end time is required")
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        return {"date": v}
    out: dict[str, Any] = {"dateTime": v}
    if timezone:
        out["timeZone"] = timezone
    return out


def _attendees(value: Any) -> list[dict[str, str]] | None:
    if not value:
        return None
    if isinstance(value, str):
        emails = [x.strip() for x in value.split(",")]
    elif isinstance(value, list):
        emails = [str(x).strip() for x in value]
    else:
        raise ValueError("attendees must be a comma-separated string or list")
    emails = [x for x in emails if x]
    return [{"email": x} for x in emails] if emails else None


def _event_body(args: dict[str, Any], partial: bool = False) -> dict[str, Any]:
    body: dict[str, Any] = {}
    if args.get("title") is not None:
        body["summary"] = args.get("title") or "Untitled event"
    elif not partial:
        raise ValueError("title is required")
    if args.get("description") is not None:
        body["description"] = args.get("description") or ""
    if args.get("location") is not None:
        body["location"] = args.get("location") or ""
    timezone = args.get("timezone") or args.get("timezone_str") or None
    if args.get("start_datetime") is not None:
        body["start"] = _event_time(str(args.get("start_datetime") or ""), timezone)
    elif not partial:
        raise ValueError("start_datetime is required")
    if args.get("end_datetime") is not None:
        body["end"] = _event_time(str(args.get("end_datetime") or ""), timezone)
    elif not partial:
        raise ValueError("end_datetime is required")
    attendees = _attendees(args.get("attendees"))
    if attendees is not None:
        body["attendees"] = attendees
    return body


def calendar_create_event(args: dict[str, Any]) -> dict[str, Any]:
    if args.get("confirm_write") is not True:
        raise ValueError("confirm_write=true is required to create calendar events")
    require_oauth_scope(args, CALENDAR_EVENTS_SCOPE)
    service = calendar_service(args)
    calendar_id = args.get("calendar_id") or "primary"
    event = service.events().insert(
        calendarId=calendar_id,
        body=_event_body(args, partial=False),
        sendUpdates=args.get("send_updates") or "all",
    ).execute()
    return {"status": "success", "event": {"id": event.get("id"), "summary": event.get("summary"), "start": event.get("start"), "end": event.get("end"), "htmlLink": event.get("htmlLink"), "status": event.get("status")}}


def calendar_update_event(args: dict[str, Any]) -> dict[str, Any]:
    if args.get("confirm_write") is not True:
        raise ValueError("confirm_write=true is required to update calendar events")
    require_oauth_scope(args, CALENDAR_EVENTS_SCOPE)
    service = calendar_service(args)
    calendar_id = args.get("calendar_id") or "primary"
    event_id = args.get("event_id") or ""
    if not event_id:
        raise ValueError("event_id is required")
    body = _event_body(args, partial=True)
    if not body:
        return {"status": "error", "message": "No update fields provided."}
    event = service.events().patch(
        calendarId=calendar_id,
        eventId=event_id,
        body=body,
        sendUpdates=args.get("send_updates") or "all",
    ).execute()
    return {"status": "success", "event": {"id": event.get("id"), "summary": event.get("summary"), "start": event.get("start"), "end": event.get("end"), "htmlLink": event.get("htmlLink"), "status": event.get("status")}}


def gmail_service(args: dict[str, Any]):
    requested_subject(args)
    return build("gmail", "v1", credentials=oauth_credentials(args), cache_discovery=False)


def gmail_search(args: dict[str, Any]) -> dict[str, Any]:
    service = gmail_service(args)
    q = args.get("query") or ""
    max_results = min(int(args.get("max_results") or 10), 100)
    res = service.users().messages().list(userId="me", q=q, maxResults=max_results).execute()
    messages = []
    for m in res.get("messages", []):
        md = service.users().messages().get(userId="me", id=m["id"], format="metadata", metadataHeaders=["From", "To", "Subject", "Date"]).execute()
        headers = {h.get("name", "").lower(): h.get("value", "") for h in md.get("payload", {}).get("headers", [])}
        messages.append({
            "id": md.get("id"),
            "threadId": md.get("threadId"),
            "snippet": md.get("snippet"),
            "from": headers.get("from"),
            "to": headers.get("to"),
            "subject": headers.get("subject"),
            "date": headers.get("date"),
        })
    return {"status": "success", "count": len(messages), "messages": messages}


def decode_body(payload: dict[str, Any]) -> str:
    def walk(node: dict[str, Any], wanted: str) -> str | None:
        if node.get("mimeType") == wanted and node.get("body", {}).get("data"):
            raw = node["body"]["data"]
            raw += "=" * (-len(raw) % 4)
            return base64.urlsafe_b64decode(raw).decode("utf-8", errors="replace")
        for p in node.get("parts", []) or []:
            found = walk(p, wanted)
            if found is not None:
                return found
        return None
    return walk(payload, "text/plain") or walk(payload, "text/html") or ""


def _addr_list(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, str):
        parts = [x.strip() for x in value.split(",")]
    elif isinstance(value, list):
        parts = [str(x).strip() for x in value]
    else:
        raise ValueError("email recipients must be a comma-separated string or list")
    return ", ".join([x for x in parts if x])


def gmail_create_draft(args: dict[str, Any]) -> dict[str, Any]:
    if args.get("confirm_write") is not True:
        raise ValueError("confirm_write=true is required to create Gmail drafts")
    require_oauth_scope(args, GMAIL_COMPOSE_SCOPE)
    to_addr = _addr_list(args.get("to"))
    subject = str(args.get("email_subject") or args.get("subject") or "")
    body = str(args.get("body") or "")
    if not to_addr:
        raise ValueError("to is required")
    if not subject:
        raise ValueError("subject is required")
    if not body:
        raise ValueError("body is required")

    msg = EmailMessage()
    msg["To"] = to_addr
    if args.get("cc"):
        msg["Cc"] = _addr_list(args.get("cc"))
    if args.get("bcc"):
        msg["Bcc"] = _addr_list(args.get("bcc"))
    msg["Subject"] = subject
    msg.set_content(body)

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("ascii")
    service = gmail_service(args)
    draft = service.users().drafts().create(userId="me", body={"message": {"raw": raw}}).execute()
    message = draft.get("message", {})
    return {
        "status": "success",
        "draft": {
            "id": draft.get("id"),
            "messageId": message.get("id"),
            "threadId": message.get("threadId"),
        },
        "sent": False,
        "note": "Draft created only. No email was sent.",
    }


def gmail_read(args: dict[str, Any]) -> dict[str, Any]:
    service = gmail_service(args)
    mid = args.get("message_id") or ""
    msg = service.users().messages().get(userId="me", id=mid, format="full").execute()
    headers = {h.get("name", "").lower(): h.get("value", "") for h in msg.get("payload", {}).get("headers", [])}
    return {
        "status": "success",
        "message": {
            "id": msg.get("id"),
            "threadId": msg.get("threadId"),
            "from": headers.get("from"),
            "to": headers.get("to"),
            "subject": headers.get("subject"),
            "date": headers.get("date"),
            "snippet": msg.get("snippet"),
            "body": decode_body(msg.get("payload", {})),
        },
    }


ACTIONS = {
    "drive_list": drive_list,
    "drive_metadata": drive_metadata,
    "drive_download": drive_download,
    "drive_upload_text": drive_upload_text,
    "drive_upload_file": drive_upload_file,
    "drive_update_file": drive_update_file,
    "drive_create_folder": drive_create_folder,
    "sheets_list_tabs": sheets_list_tabs,
    "sheets_read": sheets_read,
    "sheets_append": sheets_append,
    "sheets_write": sheets_write,
    "sheets_create_tab": sheets_create_tab,
    "calendar_list": calendar_list,
    "calendar_events": calendar_events,
    "calendar_create_event": calendar_create_event,
    "calendar_update_event": calendar_update_event,
    "gmail_search": gmail_search,
    "gmail_read": gmail_read,
    "gmail_create_draft": gmail_create_draft,
}


WRITE_ACTIONS = {"drive_upload_text", "drive_upload_file", "drive_update_file", "drive_create_folder", "sheets_append", "sheets_write", "sheets_create_tab", "calendar_create_event", "calendar_update_event", "gmail_create_draft"}


def dispatch(action: str, args: dict[str, Any]) -> dict[str, Any]:
    if action not in ACTIONS:
        raise ValueError("Unknown Google action")
    resolve_oauth_email(args)
    private = os.environ.get("PI_EXTERNAL_MCP_PRIVATE") == "1"
    if not private and (action.startswith("gmail_") or action.startswith("calendar_")):
        if not (os.environ.get("PI_EXTERNAL_PLATFORM") == "slack" and action == "gmail_create_draft"):
            return {"status": "blocked", "message": "Ask in DM for Gmail/Calendar access. Shared Slack permits Gmail draft creation only."}
    if action in WRITE_ACTIONS and args.get("confirm_write") is not True:
        raise ValueError("confirm_write=true requires the caller's explicit request for this write")
    result = ACTIONS[action](args)
    if not private and action == "gmail_create_draft":
        return {"status": result["status"], "sent": False, "message": "Draft created. No email sent."}
    return result


def main() -> int:
    action = sys.argv[1] if len(sys.argv) > 1 else ""
    try:
        import socket
        socket.setdefaulttimeout(30)
        args = json.loads(sys.stdin.read(2_000_001) or "{}")
        out(dispatch(action, args))
        return 0
    except HttpError as e:
        out(safe_http_error(e))
        return 1
    except (ValueError, KeyError, OSError) as e:
        # Only locally generated validation errors may reach the model. Never
        # serialize provider exceptions, request URLs, credentials or tracebacks.
        message = str(e) if isinstance(e, ValueError) else "Google access unavailable. Send `google login` in a DM."
        fail(message[:300])
        return 1
    except Exception:
        fail("Google request failed. Check access or send `google login` in a DM.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
