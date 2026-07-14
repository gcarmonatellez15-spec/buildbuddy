package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/git_trace2"
	"github.com/stretchr/testify/require"
)

func TestGitFetchTraceProcessor_TotalBytes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		events []git_trace2.Event
		want   int64
	}{
		{
			name: "fetch with no new objects",
			events: []git_trace2.Event{
				{Type: "start", SessionID: "sid1", Thread: "main"},
				{Type: "data", SessionID: "sid1", Thread: "main", Category: "transfer", Key: "negotiated-version", Value: json.RawMessage(`"2"`)},
				{Type: "exit", SessionID: "sid1", Thread: "main"},
			},
		},
		{
			// A completed receive reports its exact pack size. Object counts and
			// data from unrelated categories are not byte counts.
			name: "received pack",
			events: []git_trace2.Event{
				{Type: "region_enter", SessionID: "sid1/sid2", Thread: "main", Category: "progress", Label: "Receiving objects"},
				{Type: "data", SessionID: "sid1/sid2", Thread: "main", Category: "progress", Key: "total_objects", Value: json.RawMessage(`"155"`)},
				{Type: "data", SessionID: "sid1/sid2", Thread: "main", Category: "progress", Key: "total_bytes", Value: json.RawMessage(`"12985331"`)},
				{Type: "data", SessionID: "sid1/sid2", Thread: "main", Category: "pack", Key: "total_bytes", Value: json.RawMessage(`"999"`)},
				{Type: "region_leave", SessionID: "sid1/sid2", Thread: "main", Category: "progress", Label: "Receiving objects"},
			},
			want: 12985331,
		},
		{
			// Child Git processes connect independently to the Trace2 listener.
			// Their completed pack sizes are added together.
			name: "multiple packs",
			events: []git_trace2.Event{
				{Type: "data", SessionID: "sid1/sid2", Thread: "main", Category: "progress", Key: "total_bytes", Value: json.RawMessage(`"1000"`)},
				{Type: "data", SessionID: "sid1/sid3", Thread: "main", Category: "progress", Key: "total_bytes", Value: json.RawMessage(`234`)},
			},
			want: 1234,
		},
		{
			// An unexpected value should not prevent subsequent events from being
			// processed.
			name: "invalid values skipped",
			events: []git_trace2.Event{
				{Type: "data", SessionID: "sid1/sid2", Thread: "main", Category: "progress", Key: "total_bytes", Value: json.RawMessage(`"not-a-number"`)},
				{Type: "data", SessionID: "sid1/sid2", Thread: "main", Category: "progress", Key: "total_bytes", Value: json.RawMessage(`"4096"`)},
			},
			want: 4096,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			processor := newGitFetchTraceProcessor(gitFetchOptions{rateSamplePeriod: time.Hour}, false, func() {})
			for _, event := range tc.events {
				processor.observeTrace2Event(event)
			}
			_, got := processor.stop()
			require.Equal(t, tc.want, got)
		})
	}
}

func TestGitFetchTraceProcessor_SustainedSlowRateTimesOut(t *testing.T) {
	// The pack trace receives 100 bytes per second for ten seconds while the
	// Trace2 Receiving objects region is active. Expect the fetch to be canceled.
	ctx, cancel := context.WithCancel(t.Context())
	processor := newGitFetchTraceProcessor(gitFetchOptions{
		slowRateBytesPerSecond: 200,
		slowTimeout:            10 * time.Second,
		rateSamplePeriod:       time.Hour,
	}, true, cancel)
	start := time.Unix(100, 0)
	processor.now = func() time.Time { return start }
	processor.observeTrace2Event(receivingObjectsEvent("region_enter"))
	_, err := processor.Write([]byte{0})
	require.NoError(t, err)

	_, err = processor.Write(make([]byte, 500))
	require.NoError(t, err)
	processor.sampleRate(start.Add(5 * time.Second))
	require.NoError(t, ctx.Err())

	_, err = processor.Write(make([]byte, 500))
	require.NoError(t, err)
	processor.sampleRate(start.Add(10 * time.Second))
	require.ErrorIs(t, ctx.Err(), context.Canceled)

	timedOut, totalBytes := processor.stop()
	require.True(t, timedOut)
	require.Equal(t, int64(1001), totalBytes)
}

func TestGitFetchTraceProcessor_FastRateResetsSlowPeriod(t *testing.T) {
	// A healthy five-second sample separates two slow samples. Verify the slow
	// duration starts over instead of carrying across the healthy transfer.
	ctx, cancel := context.WithCancel(t.Context())
	processor := newGitFetchTraceProcessor(gitFetchOptions{
		slowRateBytesPerSecond: 200,
		slowTimeout:            10 * time.Second,
		rateSamplePeriod:       time.Hour,
	}, true, cancel)
	start := time.Unix(100, 0)
	processor.now = func() time.Time { return start }
	processor.observeTrace2Event(receivingObjectsEvent("region_enter"))
	_, err := processor.Write([]byte{0})
	require.NoError(t, err)

	_, err = processor.Write(make([]byte, 500))
	require.NoError(t, err)
	processor.sampleRate(start.Add(5 * time.Second))

	_, err = processor.Write(make([]byte, 2_000))
	require.NoError(t, err)
	processor.sampleRate(start.Add(10 * time.Second))

	_, err = processor.Write(make([]byte, 500))
	require.NoError(t, err)
	processor.sampleRate(start.Add(15 * time.Second))
	require.NoError(t, ctx.Err())

	processor.stop()
}

func TestGitFetchTraceProcessor_CompletedReceiveStopsMonitoring(t *testing.T) {
	// Delta resolution can continue after all pack bytes arrive. Once Trace2
	// leaves Receiving objects, a quiet pipe must not cancel that CPU-only work.
	ctx, cancel := context.WithCancel(t.Context())
	processor := newGitFetchTraceProcessor(gitFetchOptions{
		slowRateBytesPerSecond: 200,
		slowTimeout:            10 * time.Second,
		rateSamplePeriod:       time.Hour,
	}, true, cancel)
	start := time.Unix(100, 0)
	processor.now = func() time.Time { return start }
	processor.observeTrace2Event(receivingObjectsEvent("region_enter"))
	_, err := processor.Write([]byte{0})
	require.NoError(t, err)
	_, err = processor.Write(make([]byte, 500))
	require.NoError(t, err)
	processor.sampleRate(start.Add(5 * time.Second))

	processor.observeTrace2Event(receivingObjectsEvent("region_leave"))
	processor.sampleRate(start.Add(20 * time.Second))
	require.NoError(t, ctx.Err())

	processor.stop()
}

func TestRunGitFetch_Trace2SocketCollectsMetrics(t *testing.T) {
	// Slow detection is disabled by default, so no raw pack trace is requested.
	// Trace2 child connections should still report the completed byte totals.
	run := func(ctx context.Context, out io.Writer, env map[string]string, extraFiles []*os.File, args ...string) (string, *commandError) {
		require.Empty(t, extraFiles)
		for _, bytes := range []int64{1000, 234} {
			conn := dialGitTrace2(t, env)
			writeTrace2Event(t, conn, git_trace2.Event{
				Type:      "data",
				SessionID: "child",
				Thread:    "main",
				Category:  "progress",
				Key:       "total_bytes",
				Value:     json.RawMessage(strconv.Quote(strconv.FormatInt(bytes, 10))),
			})
			require.NoError(t, conn.Close())
		}
		return "fetch complete", nil
	}

	result := runGitFetch(t.Context(), io.Discard, gitFetchOptions{}, run, "fetch")

	require.Nil(t, result.err)
	require.Equal(t, "fetch complete", result.output)
	require.Equal(t, int64(1234), result.totalBytes)
}

func TestRunGitFetch_RetriesAfterSustainedSlowRate(t *testing.T) {
	// Each attempt starts Receiving objects, emits a pack byte, and then stalls.
	// The initial attempt and both configured retries should run.
	attempts := 0
	run := func(ctx context.Context, out io.Writer, env map[string]string, extraFiles []*os.File, args ...string) (string, *commandError) {
		attempts++
		conn := dialGitTrace2(t, env)
		defer conn.Close()
		writeTrace2Event(t, conn, receivingObjectsEvent("region_enter"))
		require.Len(t, extraFiles, 1)
		_, err := extraFiles[0].Write([]byte{0})
		require.NoError(t, err)
		<-ctx.Done()
		return "", &commandError{Err: ctx.Err()}
	}

	result := runGitFetch(t.Context(), io.Discard, gitFetchOptions{
		slowRateBytesPerSecond: 200,
		slowTimeout:            5 * time.Millisecond,
		retries:                2,
		retryDelay:             time.Millisecond,
		rateSamplePeriod:       time.Millisecond,
	}, run, "fetch")

	require.NotNil(t, result.err)
	require.Equal(t, 3, attempts)
	require.Equal(t, int64(3), result.totalBytes)
}

func TestRunGitFetch_ReturnsSuccessfulRetry(t *testing.T) {
	// The first attempt stalls. The second completes and emits matching pack
	// trace and Trace2 totals, which should be counted once for that attempt.
	attempts := 0
	run := func(ctx context.Context, out io.Writer, env map[string]string, extraFiles []*os.File, args ...string) (string, *commandError) {
		attempts++
		conn := dialGitTrace2(t, env)
		defer conn.Close()
		writeTrace2Event(t, conn, receivingObjectsEvent("region_enter"))
		require.Len(t, extraFiles, 1)
		if attempts == 1 {
			_, err := extraFiles[0].Write([]byte{0})
			require.NoError(t, err)
			<-ctx.Done()
			return "", &commandError{Err: ctx.Err()}
		}

		_, err := extraFiles[0].Write(make([]byte, 1000))
		require.NoError(t, err)
		writeTrace2Event(t, conn, git_trace2.Event{
			Type:      "data",
			SessionID: "sid",
			Thread:    "main",
			Category:  "progress",
			Key:       "total_bytes",
			Value:     json.RawMessage(`"1000"`),
		})
		writeTrace2Event(t, conn, receivingObjectsEvent("region_leave"))
		return "fetch complete", nil
	}

	result := runGitFetch(t.Context(), io.Discard, gitFetchOptions{
		slowRateBytesPerSecond: 200,
		slowTimeout:            5 * time.Millisecond,
		retries:                2,
		retryDelay:             time.Millisecond,
		rateSamplePeriod:       time.Millisecond,
	}, run, "fetch")

	require.Nil(t, result.err)
	require.Equal(t, "fetch complete", result.output)
	require.Equal(t, 2, attempts)
	require.Equal(t, int64(1001), result.totalBytes)
}

func TestRunGitFetch_DoesNotRetryOrdinaryFailure(t *testing.T) {
	// An authentication failure has no slow-transfer timeout marker, so it
	// should be returned immediately.
	attempts := 0
	wantErr := errors.New("authentication failed")
	run := func(ctx context.Context, out io.Writer, env map[string]string, extraFiles []*os.File, args ...string) (string, *commandError) {
		attempts++
		return "", &commandError{Err: wantErr, Output: "fatal: authentication failed"}
	}

	result := runGitFetch(t.Context(), io.Discard, gitFetchOptions{
		slowRateBytesPerSecond: 200,
		slowTimeout:            5 * time.Millisecond,
		retries:                2,
		retryDelay:             time.Millisecond,
		rateSamplePeriod:       time.Millisecond,
	}, run, "fetch")

	require.NotNil(t, result.err)
	require.Equal(t, wantErr, result.err.Err)
	require.Equal(t, 1, attempts)
}

func receivingObjectsEvent(event string) git_trace2.Event {
	return git_trace2.Event{
		Type:      event,
		SessionID: "sid",
		Thread:    "main",
		Category:  "progress",
		Label:     "Receiving objects",
	}
}

func dialGitTrace2(t *testing.T, env map[string]string) net.Conn {
	target := env["GIT_TRACE2_EVENT"]
	const prefix = "af_unix:stream:"
	require.True(t, strings.HasPrefix(target, prefix))
	conn, err := net.Dial("unix", strings.TrimPrefix(target, prefix))
	require.NoError(t, err)
	return conn
}

func writeTrace2Event(t *testing.T, w io.Writer, event git_trace2.Event) {
	b, err := json.Marshal(event)
	require.NoError(t, err)
	b = append(b, '\n')
	_, err = w.Write(b)
	require.NoError(t, err)
}
