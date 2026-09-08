package logstream

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"ephemera/internal/driver"
)

var base = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func line(text string) driver.LogLine {
	return driver.LogLine{At: base, Stream: driver.StreamStdout, Text: text}
}

// collect drains a channel until it closes or the deadline passes.
func collect(t *testing.T, ch <-chan driver.LogLine, want int, within time.Duration) []driver.LogLine {
	t.Helper()
	var out []driver.LogLine
	deadline := time.After(within)
	for len(out) < want {
		select {
		case l, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, l)
		case <-deadline:
			return out
		}
	}
	return out
}

func texts(lines []driver.LogLine) []string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = l.Text
	}
	return out
}

func TestSubscriberReceivesLiveLines(t *testing.T) {
	b := NewBroker()
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	ch, cancel := b.Subscribe(ctx, "job_1")
	defer cancel()

	b.Publish("job_1", line("first"))
	b.Publish("job_1", line("second"))

	got := collect(t, ch, 2, 2*time.Second)
	if len(got) != 2 {
		t.Fatalf("received %d lines, want 2: %v", len(got), texts(got))
	}
	if got[0].Text != "first" || got[1].Text != "second" {
		t.Errorf("lines out of order: %v", texts(got))
	}
}

// TestLateSubscriberGetsHistory: viewers arrive late. Someone opening a
// dashboard mid-job wants to see what happened, not an empty pane.
func TestLateSubscriberGetsHistory(t *testing.T) {
	b := NewBroker()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		b.Publish("job_1", line(fmt.Sprintf("line-%d", i)))
	}

	ch, cancel := b.Subscribe(ctx, "job_1")
	defer cancel()

	got := collect(t, ch, 5, 2*time.Second)
	if len(got) != 5 {
		t.Fatalf("late subscriber got %d lines, want the 5 already published: %v", len(got), texts(got))
	}
	if got[0].Text != "line-0" {
		t.Errorf("history out of order: %v", texts(got))
	}

	// And it continues live from there.
	b.Publish("job_1", line("live"))
	more := collect(t, ch, 1, 2*time.Second)
	if len(more) != 1 || more[0].Text != "live" {
		t.Errorf("live line after history = %v", texts(more))
	}
}

func TestHistoryIsBounded(t *testing.T) {
	b := NewBroker(WithHistorySize(10))

	for i := 0; i < 100; i++ {
		b.Publish("job_1", line(fmt.Sprintf("line-%d", i)))
	}

	history := b.History("job_1")
	if len(history) != 10 {
		t.Fatalf("history = %d lines, want the bound of 10", len(history))
	}
	// The window kept must be the most recent one.
	if history[0].Text != "line-90" || history[9].Text != "line-99" {
		t.Errorf("wrong window retained: %v ... %v", history[0].Text, history[9].Text)
	}
}

// TestSlowSubscriberNeverBlocksPublisher is the central guarantee: a viewer on
// a bad connection degrades itself, not the job.
func TestSlowSubscriberNeverBlocksPublisher(t *testing.T) {
	b := NewBroker(WithSubscriberBuffer(4))
	ctx := context.Background()

	// Subscribe and then never read.
	_, cancel := b.Subscribe(ctx, "job_1")
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10000; i++ {
			b.Publish("job_1", line(fmt.Sprintf("line-%d", i)))
		}
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("publishing blocked on a subscriber that stopped reading")
	}
}

// TestDroppedLinesAreDeclared: an operator reading a log that quietly omits
// lines will draw wrong conclusions. A visible gap marker is honest.
func TestDroppedLinesAreDeclared(t *testing.T) {
	b := NewBroker(WithSubscriberBuffer(2), WithHistorySize(1))
	ctx := context.Background()

	ch, cancel := b.Subscribe(ctx, "job_1")
	defer cancel()

	// Overflow the subscriber's buffer while nobody is reading.
	for i := 0; i < 50; i++ {
		b.Publish("job_1", line(fmt.Sprintf("line-%d", i)))
	}

	// Now drain, and give the broker a chance to deliver the notice.
	b.Publish("job_1", line("after"))
	got := collect(t, ch, 10, 2*time.Second)

	var sawNotice bool
	for _, l := range got {
		if l.Stream == StreamNotice && strings.Contains(l.Text, "dropped") {
			sawNotice = true
		}
	}
	if !sawNotice {
		t.Errorf("no drop notice delivered after overflow; lines were lost silently: %v", texts(got))
	}
}

func TestEndClosesSubscribers(t *testing.T) {
	b := NewBroker()
	ctx := context.Background()

	ch, cancel := b.Subscribe(ctx, "job_1")
	defer cancel()

	b.Publish("job_1", line("only"))
	b.End("job_1")

	// The channel must close, or an SSE handler would hang forever holding a
	// connection open for a job that finished.
	deadline := time.After(2 * time.Second)
	closed := false
	for !closed {
		select {
		case _, ok := <-ch:
			if !ok {
				closed = true
			}
		case <-deadline:
			t.Fatal("subscriber channel did not close after End")
		}
	}

	if !b.Ended("job_1") {
		t.Error("Ended() = false after End()")
	}
	if b.SubscriberCount("job_1") != 0 {
		t.Error("subscribers not detached after End")
	}
}

// TestSubscribeAfterEndReplaysAndCloses: an operator investigating a failure
// arrives after the job is over and must still see the tail.
func TestSubscribeAfterEndReplaysAndCloses(t *testing.T) {
	b := NewBroker()
	ctx := context.Background()

	b.Publish("job_1", line("what"))
	b.Publish("job_1", line("happened"))
	b.End("job_1")

	ch, cancel := b.Subscribe(ctx, "job_1")
	defer cancel()

	var got []driver.LogLine
	deadline := time.After(2 * time.Second)
	for {
		select {
		case l, ok := <-ch:
			if !ok {
				if len(got) != 2 {
					t.Fatalf("replayed %d lines, want 2: %v", len(got), texts(got))
				}
				return
			}
			got = append(got, l)
		case <-deadline:
			t.Fatal("channel never closed for an ended job")
		}
	}
}

func TestForgetReleasesHistory(t *testing.T) {
	b := NewBroker()
	b.Publish("job_1", line("x"))
	if len(b.History("job_1")) == 0 {
		t.Fatal("history not retained")
	}

	// Without Forget, a long-running control plane accumulates history for
	// every job it has ever run - a slow leak that only shows in production.
	b.Forget("job_1")
	if len(b.History("job_1")) != 0 {
		t.Error("history survived Forget")
	}
	if b.SubscriberCount("job_1") != 0 {
		t.Error("topic survived Forget")
	}
}

func TestCancelDetachesSubscriber(t *testing.T) {
	b := NewBroker()
	ctx := context.Background()

	_, cancel := b.Subscribe(ctx, "job_1")
	if b.SubscriberCount("job_1") != 1 {
		t.Fatalf("SubscriberCount = %d, want 1", b.SubscriberCount("job_1"))
	}

	cancel()
	if b.SubscriberCount("job_1") != 0 {
		t.Errorf("SubscriberCount = %d after cancel, want 0", b.SubscriberCount("job_1"))
	}
	// Cancelling twice must not panic on a double close.
	cancel()
}

func TestContextCancellationDetaches(t *testing.T) {
	b := NewBroker()
	ctx, cancelCtx := context.WithCancel(context.Background())

	ch, cancel := b.Subscribe(ctx, "job_1")
	defer cancel()

	cancelCtx()

	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // closed as expected
			}
		case <-deadline:
			t.Fatal("channel did not close when the context was cancelled")
		}
	}
}

func TestJobsAreIsolated(t *testing.T) {
	b := NewBroker()
	ctx := context.Background()

	chA, cancelA := b.Subscribe(ctx, "job_a")
	defer cancelA()
	chB, cancelB := b.Subscribe(ctx, "job_b")
	defer cancelB()

	b.Publish("job_a", line("for-a"))

	got := collect(t, chA, 1, 2*time.Second)
	if len(got) != 1 || got[0].Text != "for-a" {
		t.Errorf("job_a subscriber got %v", texts(got))
	}

	select {
	case l := <-chB:
		t.Errorf("job_b subscriber received a line meant for job_a: %q", l.Text)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestNoteMarksPlatformLines(t *testing.T) {
	b := NewBroker(WithClock(func() time.Time { return base }))
	ctx := context.Background()

	ch, cancel := b.Subscribe(ctx, "job_1")
	defer cancel()

	b.Note("job_1", "provisioning environment for %s", "tenant-a")

	got := collect(t, ch, 1, 2*time.Second)
	if len(got) != 1 {
		t.Fatalf("got %d lines", len(got))
	}
	// Platform lifecycle events share the agent's timeline, so an operator
	// reads one story rather than two, but stay distinguishable.
	if got[0].Stream != StreamNotice {
		t.Errorf("Stream = %q, want %q", got[0].Stream, StreamNotice)
	}
	if !strings.Contains(got[0].Text, "tenant-a") {
		t.Errorf("Text = %q", got[0].Text)
	}
	if !got[0].At.Equal(base) {
		t.Errorf("At = %v, want the injected clock value", got[0].At)
	}
}

func TestMultipleSubscribersAllReceive(t *testing.T) {
	b := NewBroker()
	ctx := context.Background()

	const viewers = 8
	chans := make([]<-chan driver.LogLine, viewers)
	for i := 0; i < viewers; i++ {
		ch, cancel := b.Subscribe(ctx, "job_1")
		defer cancel()
		chans[i] = ch
	}

	b.Publish("job_1", line("broadcast"))

	for i, ch := range chans {
		got := collect(t, ch, 1, 2*time.Second)
		if len(got) != 1 || got[0].Text != "broadcast" {
			t.Errorf("viewer %d got %v", i, texts(got))
		}
	}
}

// TestConcurrentPublishAndSubscribe is the shape the API produces: viewers
// attaching and detaching while a job floods output. Run with -race, this
// catches a missing lock.
func TestConcurrentPublishAndSubscribe(t *testing.T) {
	b := NewBroker(WithSubscriberBuffer(16), WithHistorySize(64))
	ctx := context.Background()

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			b.Publish("job_1", line(fmt.Sprintf("line-%d", i)))
		}
	}()

	for v := 0; v < 10; v++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				ch, cancel := b.Subscribe(ctx, "job_1")
				select {
				case <-ch:
				case <-time.After(50 * time.Millisecond):
				}
				cancel()
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	b.End("job_1")
	if b.SubscriberCount("job_1") != 0 {
		t.Errorf("SubscriberCount = %d, want 0 after all viewers detached", b.SubscriberCount("job_1"))
	}
}
