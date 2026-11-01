package telegram

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
)

func TestCustomAPIFilePrefixAndLimit(t *testing.T) {
	var apiCalls, fileCalls, reportedSize atomic.Int32
	reportedSize.Store(4)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/botfake/getFile" {
			t.Errorf("API path %s", r.URL.Path)
		}
		apiCalls.Add(1)
		fmt.Fprintf(w, `{"ok":true,"result":{"file_path":"doc.txt","file_size":%d}}`, reportedSize.Load())
	}))
	defer api.Close()
	files := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/custom/file/botfake/doc.txt" {
			t.Errorf("file path %s", r.URL.Path)
		}
		fileCalls.Add(1)
		fmt.Fprint(w, "data")
	}))
	defer files.Close()
	tg, err := New(Options{Token: "fake", BotUsername: "example_bot", API: api.URL, FileBase: files.URL + "/custom/file/bot", MaxFileBytes: 4, UploadDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	path, n, err := tg.download(context.Background(), &attachment{FileID: "id", Size: 4})
	if err != nil || n != 4 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	b, err := os.ReadFile(path)
	if err != nil || string(b) != "data" {
		t.Fatalf("download %q: %v", b, err)
	}
	tg.opt.MaxFileBytes = 3
	if _, _, err := tg.download(context.Background(), &attachment{FileID: "id", Size: 4}); err == nil {
		t.Fatal("reported oversize accepted")
	}
	if _, _, err := tg.download(context.Background(), &attachment{FileID: "id"}); err == nil {
		t.Fatal("getFile oversize accepted")
	}
	reportedSize.Store(0)
	if _, _, err := tg.download(context.Background(), &attachment{FileID: "id"}); err == nil {
		t.Fatal("streamed oversize accepted")
	}
	entries, err := os.ReadDir(tg.opt.UploadDir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("partial download retained: entries=%d err=%v", len(entries), err)
	}
	if apiCalls.Load() != 3 || fileCalls.Load() != 2 {
		t.Fatalf("calls API=%d files=%d", apiCalls.Load(), fileCalls.Load())
	}
}
