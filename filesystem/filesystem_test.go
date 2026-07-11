package filesystem

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/rezarajan/eventflow/adaptertest"
)

func TestEmitterReceiverFiles(t *testing.T) {
	dir := t.TempDir()
	event := adaptertest.NewTestEvent()
	emitter := NewEmitter(Config{Path: dir, Mode: ModeFiles, Atomic: true})
	adaptertest.RunEmitterContract(t, emitter, event)

	receiver := NewReceiver(Config{Path: dir, Mode: ModeFiles})
	adaptertest.RunReceiverContract(t, receiver)
}

func TestEmitterWritesCommitMarker(t *testing.T) {
	dir := t.TempDir()
	emitter := NewEmitter(Config{Path: dir, Mode: ModeFiles, CommitMarker: "_COMMIT"})
	if err := emitter.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := emitter.Emit(context.Background(), adaptertest.NewTestEvent()); err != nil {
		t.Fatal(err)
	}
	if err := emitter.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := filepath.Glob(filepath.Join(dir, "_COMMIT")); err != nil {
		t.Fatal(err)
	}
}
