package telegram

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
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

func TestAbsoluteLocalFileContract(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "photo.jpg")
	if err := os.WriteFile(source, []byte("photo"), 0600); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/botfake/getFile" {
			fmt.Fprintf(w, `{"ok":true,"result":{"file_path":%q}}`, source)
			return
		}
		http.NotFound(w, r)
	}))
	defer api.Close()
	tg, err := New(Options{Token: "fake", BotUsername: "example_bot", API: api.URL, UploadDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	// Opt-in root will be configured by the deployment; fixture root is synthetic.
	tg.opt.LocalFileRoot = root
	dst, n, err := tg.download(context.Background(), &attachment{FileID: "photo"})
	if err != nil || n != 5 {
		t.Fatalf("local getFile contract: n=%d err=%v", n, err)
	}
	b, err := os.ReadFile(dst)
	if err != nil || string(b) != "photo" {
		t.Fatalf("copy: %q %v", b, err)
	}
	if dst == source {
		t.Fatal("source path escaped private upload boundary")
	}
}
func TestHTTP404IsNotSizeEvidence(t *testing.T) {
	note := describe(&attachment{Kind: "photo"}, fmt.Errorf("file download HTTP 404"))
	if strings.Contains(note, "compressed") || strings.Contains(note, "smaller") {
		t.Fatalf("misclassified failure: %s", note)
	}
}

func TestLocalFileConfinement(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	os.WriteFile(filepath.Join(root, "ok"), []byte("ok"), 0600)
	os.WriteFile(filepath.Join(outside, "secret"), []byte("unread"), 0600)
	os.Symlink(filepath.Join(outside, "secret"), filepath.Join(root, "escape"))
	os.Symlink("ok", filepath.Join(root, "inside"))
	if err := syscall.Mkfifo(filepath.Join(root, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	tg, _ := New(Options{Token: "fake", BotUsername: "example_bot", API: "http://127.0.0.1:8081", UploadDir: t.TempDir(), MaxFileBytes: 2})
	tg.opt.LocalFileRoot = root
	for _, name := range []string{filepath.Join(root, "escape"), root, filepath.Join(root, "fifo"), root + "/../" + filepath.Base(outside) + "/secret", filepath.Join(outside, "secret"), "../secret", "documents/../../secret"} {
		t.Run(filepath.Base(name), func(t *testing.T) {
			r, err := tg.fileReader(context.Background(), name)
			if err == nil {
				r.Close()
				t.Fatal("unconfined/nonregular path accepted")
			}
		})
	}
	r, err := tg.fileReader(context.Background(), filepath.Join(root, "inside"))
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	tg.opt.MaxFileBytes = 1
	if r, err := tg.fileReader(context.Background(), filepath.Join(root, "ok")); err == nil {
		r.Close()
		t.Fatal("oversize accepted")
	}
	tg.opt.MaxFileBytes = 2
	tg.opt.API = DefaultAPI
	if r, err := tg.fileReader(context.Background(), filepath.Join(root, "ok")); err == nil {
		r.Close()
		t.Fatal("public API local read allowed")
	}
	tg.opt.API = "http://127.0.0.1:8081"
	tg.opt.LocalFileRoot = ""
	if r, err := tg.fileReader(context.Background(), filepath.Join(root, "ok")); err == nil {
		r.Close()
		t.Fatal("unconfigured local read allowed")
	}
	for _, o := range []Options{
		{Token: "fake", BotUsername: "example_bot", API: DefaultAPI, LocalFileRoot: "/var/lib/telegram-bot-api"},
		{Token: "fake", BotUsername: "example_bot", API: "http://127.0.0.1:8081", LocalFileRoot: "/tmp"},
	} {
		if _, err := New(o); err == nil {
			t.Fatal("unsafe local root configuration accepted")
		}
	}
}
