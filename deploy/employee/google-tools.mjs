// Tool schemas ported from hub google_workspace_sa.ts (3e28082).
// Enrollment and refresh are root-only Go code, never extension tools.
export function registerGoogleTools(pi, Type, toolResult) {
	pi.registerTool({
		name: "google_drive_list",
		label: "Google Drive list",
		description: "List/search Google Drive files visible to the connected caller. Read-only. Output is truncated to 2000 lines or 50KB.",
		promptSnippet: "List/search Google Drive files visible to the connected caller",
		promptGuidelines: [
			"Use google_drive_list for Google Drive search/listing; never ask for or print Google service-account credentials.",
			"For connectivity/status checks, report only whether access works; do not reveal file names, owners, links, or other Drive metadata unless explicitly requested.",
			"Use Drive and Sheets within the caller and channel authority. Gmail and Calendar reads require a DM.",
		],
		parameters: Type.Object({
			query: Type.Optional(Type.String({ description: "Drive query, e.g. name contains 'report'. Leave empty for recent files." })),
			folder_id: Type.Optional(Type.String({ description: "Optional Drive folder URL or ID." })),
			max_results: Type.Optional(Type.Number({ description: "Max files, default 20, max 100." })),
		}),
		execute: async (_id, params, signal) => toolResult("drive_list", params, signal),
	});

	pi.registerTool({
		name: "google_drive_metadata",
		label: "Google Drive metadata",
		description: "Get metadata for a Google Drive file/folder visible to the connected caller. Read-only.",
		parameters: Type.Object({
			file_id: Type.String({ description: "Drive file/folder URL or ID." }),
		}),
		execute: async (_id, params, signal) => toolResult("drive_metadata", params, signal),
	});

	pi.registerTool({
		name: "google_drive_download",
		label: "Google Drive download",
		description: "Download/export a Google Drive file visible to the connected caller. For binary/large files, pass save_path. Read-only.",
		promptSnippet: "Download/export Google Drive files visible to the connected caller",
		parameters: Type.Object({
			file_id: Type.String({ description: "Drive file URL or ID." }),
			export_mime: Type.Optional(Type.String({ description: "Export MIME for Google Docs/Sheets/Slides, e.g. text/plain, text/csv, application/pdf." })),
			save_path: Type.Optional(Type.String({ description: "Optional local path to save bytes instead of returning content." })),
		}),
		execute: async (_id, params, signal) => toolResult("drive_download", params, signal),
	});

	pi.registerTool({
		name: "google_drive_upload_text",
		label: "Google Drive upload text",
		description: "Create/upload a text file to Google Drive using the connected caller. Write-capable; do not use unless the user asked for Drive writes.",
		promptSnippet: "Create/upload text files to Google Drive with the connected caller",
		promptGuidelines: [
			"Use google_drive_upload_text only for user-requested Google Drive writes; do not delete Drive content.",
		],
		parameters: Type.Object({
            confirm_write: Type.Boolean({description: "Must be true only when the caller explicitly requested this write."}),
			name: Type.String({ description: "Drive file name to create." }),
			content: Type.String({ description: "Text content to upload." }),
			folder_id: Type.Optional(Type.String({ description: "Optional parent folder URL or ID." })),
			mime_type: Type.Optional(Type.String({ description: "Upload MIME type, default text/plain." })),
		}),
		execute: async (_id, params, signal) => toolResult("drive_upload_text", params, signal),
	});

	pi.registerTool({
		name: "google_drive_upload_file",
		label: "Google Drive upload file",
		description: "Upload a local file to Google Drive using the connected caller. Write-capable; no delete support.",
		parameters: Type.Object({
            confirm_write: Type.Boolean({description: "Must be true only when the caller explicitly requested this write."}),
			local_path: Type.String({ description: "Local file path to upload." }),
			name: Type.Optional(Type.String({ description: "Optional Drive file name; defaults to local basename." })),
			folder_id: Type.Optional(Type.String({ description: "Optional parent folder URL or ID." })),
			mime_type: Type.Optional(Type.String({ description: "Optional source MIME type." })),
		}),
		execute: async (_id, params, signal) => toolResult("drive_upload_file", params, signal),
	});

	pi.registerTool({
		name: "google_drive_update_file",
		label: "Google Drive update file",
		description: "Replace/update Drive file content from a local file using the connected caller. Write-capable; no delete support.",
		parameters: Type.Object({
            confirm_write: Type.Boolean({description: "Must be true only when the caller explicitly requested this write."}),
			file_id: Type.String({ description: "Drive file URL or ID to update." }),
			local_path: Type.String({ description: "Local file path whose bytes replace/update Drive content." }),
			name: Type.Optional(Type.String({ description: "Optional new Drive file name." })),
			mime_type: Type.Optional(Type.String({ description: "Optional source MIME type." })),
		}),
		execute: async (_id, params, signal) => toolResult("drive_update_file", params, signal),
	});

	pi.registerTool({
		name: "google_drive_create_folder",
		label: "Google Drive create folder",
		description: "Create a Google Drive folder using the connected caller. Write-capable; no delete support.",
		parameters: Type.Object({
            confirm_write: Type.Boolean({description: "Must be true only when the caller explicitly requested this write."}),
			name: Type.String({ description: "Folder name." }),
			parent_folder_id: Type.Optional(Type.String({ description: "Optional parent folder URL or ID." })),
		}),
		execute: async (_id, params, signal) => toolResult("drive_create_folder", params, signal),
	});

	pi.registerTool({
		name: "google_sheets_list_tabs",
		label: "Google Sheets list tabs",
		description: "List tab names and IDs in a Google spreadsheet visible to the connected caller.",
		promptSnippet: "List tabs in a Google spreadsheet",
		promptGuidelines: [
			"Use google_sheets_list_tabs before writing when the target tab/range is unclear. Pass full spreadsheet URLs to avoid ID typos.",
			"For connectivity/status checks, report only whether Sheets access works; do not reveal spreadsheet or tab names unless explicitly requested.",
			"Use Sheets within the caller and channel authority; minimize private data.",
		],
		parameters: Type.Object({
			spreadsheet_id: Type.String({ description: "Spreadsheet URL or ID." }),
		}),
		execute: async (_id, params, signal) => toolResult("sheets_list_tabs", params, signal),
	});

	pi.registerTool({
		name: "google_sheets_read",
		label: "Google Sheets read",
		description: "Read values from a Google spreadsheet range.",
		parameters: Type.Object({
			spreadsheet_id: Type.String({ description: "Spreadsheet URL or ID." }),
			range: Type.Optional(Type.String({ description: "A1 range, e.g. Sheet1!A1:D10. Default A1:Z1000." })),
		}),
		execute: async (_id, params, signal) => toolResult("sheets_read", params, signal),
	});

	pi.registerTool({
		name: "google_sheets_append",
		label: "Google Sheets append",
		description: "Append rows to a Google spreadsheet. Write-capable; use only when requested/scheduled.",
		promptSnippet: "Append rows to Google Sheets",
		promptGuidelines: ["Use google_sheets_append only for requested spreadsheet writes. Prefer full spreadsheet URLs. Verify target tab/range when unclear."],
		parameters: Type.Object({
            confirm_write: Type.Boolean({description: "Must be true only when the caller explicitly requested this write."}),
			spreadsheet_id: Type.String({ description: "Spreadsheet URL or ID." }),
			range: Type.String({ description: "A1 range/table start, e.g. 'Daily progress!A:F'." }),
			values: Type.Array(Type.Array(Type.Any()), { description: "2D array of rows to append." }),
			value_input_option: Type.Optional(Type.String({ description: "RAW or USER_ENTERED. Default USER_ENTERED." })),
		}),
		execute: async (_id, params, signal) => toolResult("sheets_append", params, signal),
	});

	pi.registerTool({
		name: "google_sheets_write",
		label: "Google Sheets write",
		description: "Write/update values in a Google spreadsheet range. Write-capable; use only when requested/scheduled.",
		parameters: Type.Object({
            confirm_write: Type.Boolean({description: "Must be true only when the caller explicitly requested this write."}),
			spreadsheet_id: Type.String({ description: "Spreadsheet URL or ID." }),
			range: Type.String({ description: "A1 range, e.g. 'Report!A1:F20'." }),
			values: Type.Array(Type.Array(Type.Any()), { description: "2D array of values." }),
			value_input_option: Type.Optional(Type.String({ description: "RAW or USER_ENTERED. Default USER_ENTERED." })),
		}),
		execute: async (_id, params, signal) => toolResult("sheets_write", params, signal),
	});

	pi.registerTool({
		name: "google_sheets_create_tab",
		label: "Google Sheets create tab",
		description: "Create a new tab/sheet inside an existing Google spreadsheet. Write-capable; no delete support.",
		promptSnippet: "Create a new tab in an existing Google spreadsheet",
		parameters: Type.Object({
            confirm_write: Type.Boolean({description: "Must be true only when the caller explicitly requested this write."}),
			spreadsheet_id: Type.String({ description: "Spreadsheet URL or ID." }),
			title: Type.String({ description: "New tab title." }),
		}),
		execute: async (_id, params, signal) => toolResult("sheets_create_tab", params, signal),
	});

	pi.registerTool({
		name: "google_calendar_list",
		label: "Google Calendar list",
		description: "List calendars visible to the connected caller. Read-only.",
		promptSnippet: "List Google calendars visible to the connected caller (read-only)",
		promptGuidelines: [
			"Use google_calendar_create_event/update_event only when the user explicitly asks for a calendar event mutation.",
			"For connectivity/status checks, report only whether Calendar access works; do not reveal calendar names, event titles, attendees, email addresses, or times unless explicitly requested.",
			"Do not disclose Calendar data in shared chat/channel contexts; ask to continue in DM or provide only a safe non-sensitive summary.",
		],
		parameters: Type.Object({
			max_results: Type.Optional(Type.Number({ description: "Max calendars, default 50." })),
			email: Type.Optional(Type.String({ description: "Optional email assertion; must match your connected account." })),
		}),
		execute: async (_id, params, signal) => toolResult("calendar_list", params, signal),
	});

	pi.registerTool({
		name: "google_calendar_events",
		label: "Google Calendar events",
		description: "Read events from calendars visible to the connected caller. Read-only.",
		promptSnippet: "Read Google Calendar events visible to the connected caller (read-only)",
		parameters: Type.Object({
			calendar_id: Type.Optional(Type.String({ description: "Calendar ID, default primary." })),
			time_min: Type.Optional(Type.String({ description: "RFC3339 lower bound, e.g. 2026-05-19T00:00:00Z." })),
			time_max: Type.Optional(Type.String({ description: "RFC3339 upper bound." })),
			query: Type.Optional(Type.String({ description: "Optional full-text event search." })),
			max_results: Type.Optional(Type.Number({ description: "Max events, default 25, max 250." })),
			email: Type.Optional(Type.String({ description: "Optional email assertion; must match your connected account." })),
		}),
		execute: async (_id, params, signal) => toolResult("calendar_events", params, signal),
	});


	pi.registerTool({
		name: "google_calendar_create_event",
		label: "Google Calendar create event",
		description: "Create a Google Calendar event. Write-capable; use only when the user explicitly asks to add/create a calendar event.",
		promptSnippet: "Create a Google Calendar event",
		promptGuidelines: [
			"Use only for explicit calendar-event requests, not generic reminders or agent scheduled tasks.",
			"Confirm unclear date/time/timezone/calendar/attendees before creating.",
			"Do not disclose Calendar data in shared chat/channel contexts; ask to continue in DM or provide only a safe non-sensitive summary.",
		],
		parameters: Type.Object({
			confirm_write: Type.Boolean({ description: "Must be true. Confirms user explicitly requested Calendar event creation." }),
			title: Type.String({ description: "Event title/summary." }),
			start_datetime: Type.String({ description: "Start datetime RFC3339 or all-day date YYYY-MM-DD." }),
			end_datetime: Type.String({ description: "End datetime RFC3339 or all-day date YYYY-MM-DD." }),
			calendar_id: Type.Optional(Type.String({ description: "Calendar ID, default primary." })),
			timezone: Type.Optional(Type.String({ description: "IANA timezone, e.g. Europe/Kyiv. Recommended when datetime has no offset." })),
			description: Type.Optional(Type.String({ description: "Event description." })),
			location: Type.Optional(Type.String({ description: "Event location." })),
			attendees: Type.Optional(Type.String({ description: "Comma-separated attendee emails." })),
			send_updates: Type.Optional(Type.String({ description: "Google sendUpdates: all, externalOnly, or none. Default all." })),
			email: Type.Optional(Type.String({ description: "Optional email assertion; must match your connected account." })),
		}),
		execute: async (_id, params, signal) => toolResult("calendar_create_event", params, signal),
	});

	pi.registerTool({
		name: "google_calendar_update_event",
		label: "Google Calendar update event",
		description: "Update a Google Calendar event. Write-capable; use only when requested.",
		promptGuidelines: ["Use only for explicit calendar update requests. Confirm target event_id/calendar when unclear."],
		parameters: Type.Object({
			confirm_write: Type.Boolean({ description: "Must be true. Confirms user explicitly requested Calendar event update." }),
			event_id: Type.String({ description: "Google Calendar event ID." }),
			calendar_id: Type.Optional(Type.String({ description: "Calendar ID, default primary." })),
			title: Type.Optional(Type.String({ description: "New event title/summary." })),
			start_datetime: Type.Optional(Type.String({ description: "New start datetime RFC3339 or all-day date YYYY-MM-DD." })),
			end_datetime: Type.Optional(Type.String({ description: "New end datetime RFC3339 or all-day date YYYY-MM-DD." })),
			timezone: Type.Optional(Type.String({ description: "IANA timezone, e.g. Europe/Kyiv." })),
			description: Type.Optional(Type.String({ description: "New event description." })),
			location: Type.Optional(Type.String({ description: "New event location." })),
			attendees: Type.Optional(Type.String({ description: "Comma-separated attendee emails. Replaces attendee list when provided." })),
			send_updates: Type.Optional(Type.String({ description: "Google sendUpdates: all, externalOnly, or none. Default all." })),
			email: Type.Optional(Type.String({ description: "Optional email assertion; must match your connected account." })),
		}),
		execute: async (_id, params, signal) => toolResult("calendar_update_event", params, signal),
	});

	pi.registerTool({
		name: "google_gmail_search",
		label: "Gmail search",
		description: "Search Gmail read-only using this caller's access-only credentials.",
		promptSnippet: "Search Gmail read-only using configured OAuth tokens",
		promptGuidelines: [
			"Use google_gmail_search/google_gmail_read only for read-only Gmail access; never ask for or print Google OAuth tokens.",
			"For connectivity/status checks, report only whether Gmail access works; do not reveal senders, recipients, subjects, snippets, message IDs, or bodies unless explicitly requested.",
			"Do not disclose Gmail data in shared chat/channel contexts; ask to continue in DM or provide only a safe non-sensitive summary.",
		],
		parameters: Type.Object({
			query: Type.Optional(Type.String({ description: "Gmail search query, e.g. from:alice subject:invoice newer_than:30d." })),
			max_results: Type.Optional(Type.Number({ description: "Max messages, default 10, max 100." })),
			email: Type.Optional(Type.String({ description: "Optional email assertion; must match your connected account." })),
		}),
		execute: async (_id, params, signal) => toolResult("gmail_search", params, signal),
	});

	pi.registerTool({
		name: "google_gmail_read",
		label: "Gmail read",
		description: "Read a Gmail message body read-only using this caller's access-only credentials.",
		parameters: Type.Object({
			message_id: Type.String({ description: "Gmail message ID from google_gmail_search." }),
			email: Type.Optional(Type.String({ description: "Optional email assertion; must match your connected account." })),
		}),
		execute: async (_id, params, signal) => toolResult("gmail_read", params, signal),
	});
	pi.registerTool({
		name: "google_gmail_create_draft",
		label: "Gmail create draft",
		description: "Create a Gmail draft using the caller's connected Google account. Write-capable; creates draft only and never sends email.",
		promptSnippet: "Create a Gmail draft after explicit user request",
		promptGuidelines: [
			"Use only when the user explicitly asks to draft/compose an email.",
			"This tool creates a Gmail draft only; it must not be represented as sent email.",
			"Use the caller's own connected Google account only; never use another user's account, a connected caller, or a global fallback.",
			"In Slack shared channels, Gmail read/search/list/summarize remains DM-only. Draft creation is allowed only for an explicit draft request and must use safe status-only output: no recipient, subject, body, draft link, draft ID, or Gmail-derived content.",
			"If a shared-channel draft request lacks recipient or private details, ask minimally or tell the user to continue in DM; do not search/read Gmail to fill gaps.",
		],
		parameters: Type.Object({
			confirm_write: Type.Boolean({ description: "Must be true. Confirms user explicitly requested Gmail draft creation." }),
			to: Type.String({ description: "Recipient email address(es), comma-separated." }),
			subject: Type.String({ description: "Draft email subject." }),
			body: Type.String({ description: "Draft email plain-text body." }),
			cc: Type.Optional(Type.String({ description: "Optional CC email address(es), comma-separated." })),
			bcc: Type.Optional(Type.String({ description: "Optional BCC email address(es), comma-separated." })),
			email: Type.Optional(Type.String({ description: "Optional email assertion; must match your connected account." })),
		}),
		execute: async (_id, params, signal) => {
			const { subject: emailSubject, ...rest } = params;
			return toolResult("gmail_create_draft", { ...rest, email_subject: emailSubject }, signal);
		},
	});

}
