package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/danshapiro/kilroy/internal/attractor/gitutil"
	"github.com/danshapiro/kilroy/internal/attractor/rundb"
	"github.com/danshapiro/kilroy/internal/auth"
	"github.com/danshapiro/kilroy/internal/policy"
	"github.com/danshapiro/kilroy/internal/version"
)

type Config struct {
	Addr string
}

type Server struct {
	config  Config
	handler http.Handler
	httpSrv *http.Server
	logger  *log.Logger
}

func main() {
	cfg := Config{Addr: "127.0.0.1:8080"}
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-h", "--help", "help":
			fmt.Fprintln(os.Stderr, "usage:")
			fmt.Fprintln(os.Stderr, "  go run ./ui/server --addr 127.0.0.1:8080")
			os.Exit(0)
		case "--addr":
			i++
			if i >= len(args) {
				fmt.Fprintln(os.Stderr, "--addr requires a value")
				os.Exit(1)
			}
			cfg.Addr = args[i]
		default:
			fmt.Fprintf(os.Stderr, "unknown arg: %s\n", args[i])
			os.Exit(1)
		}
	}

	srv := New(cfg)
	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func New(cfg Config) *Server {
	if strings.TrimSpace(cfg.Addr) == "" {
		cfg.Addr = "127.0.0.1:8080"
	}
	s := &Server{
		config: cfg,
		logger: log.New(os.Stderr, "[kilroy-ui] ", log.LstdFlags),
	}
	s.handler = newHandler()
	s.httpSrv = &http.Server{
		Handler:      csrfProtect(s.handler),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
		BaseContext:  func(net.Listener) context.Context { return context.Background() },
	}
	return s
}

func newHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", handleHealth)
	mux.HandleFunc("GET /api/runs", handleListRuns)
	mux.HandleFunc("GET /api/runs/{id}", handleGetRun)
	mux.HandleFunc("GET /api/runs/{id}/outputs", handleGetRunOutputs)
	mux.HandleFunc("GET /api/runs/{id}/outputs/{name...}", handleDownloadOutput)
	mux.HandleFunc("GET /api/runs/{id}/nodes/{nodeId}/attempts", handleGetNodeAttempts)
	mux.HandleFunc("GET /api/runs/{id}/nodes/{nodeId}/turns", handleGetNodeTurns)
	mux.HandleFunc("GET /api/runs/{id}/nodes/{nodeId}/diff", handleGetNodeDiff)
	mux.HandleFunc("GET /api/runs/{id}/log", handleGetRunLog)
	mux.HandleFunc("GET /api/runs/{id}/events", handleGetRunEvents)
	mux.HandleFunc("GET /api/runs/{id}/files/{path...}", handleBrowseFiles)
	mux.HandleFunc("GET /api/runs/{id}/workspace/{path...}", handleBrowseWorkspace)
	mux.HandleFunc("GET /api/policy", handlePolicy)
	mux.HandleFunc("GET /api/policy/{class}", handlePolicyClass)
	mux.HandleFunc("GET /api/policy/explain/{id}", handlePolicyExplain)
	mux.HandleFunc("GET /api/auth", handleAuth)
	mux.Handle("GET /", uiHandler())
	return mux
}

func (s *Server) ListenAndServe() error {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		s.logger.Printf("received %s, shutting down...", sig)
		_ = s.Shutdown()
	}()

	s.logger.Printf("listening on %s", s.config.Addr)
	s.httpSrv.Addr = s.config.Addr
	err := s.httpSrv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return s.httpSrv.Shutdown(ctx)
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	status := map[string]any{
		"status":  "ok",
		"db_path": rundb.DefaultPath(),
	}
	if db, err := rundb.Open(rundb.DefaultPath()); err == nil {
		_ = db.Close()
		status["db"] = "ok"
	} else {
		status["db"] = "unavailable"
		status["warning"] = err.Error()
	}
	writeJSON(w, http.StatusOK, status)
}

func handleListRuns(w http.ResponseWriter, r *http.Request) {
	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"runs":    []any{},
			"count":   0,
			"warning": "run database unavailable: " + err.Error(),
		})
		return
	}
	defer db.Close()

	filter := rundb.ListFilter{
		Status:    r.URL.Query().Get("status"),
		GraphName: r.URL.Query().Get("graph"),
		Sort:      r.URL.Query().Get("sort"),
	}
	if labels := r.URL.Query()["label"]; len(labels) > 0 {
		filter.Labels = map[string]string{}
		for _, spec := range labels {
			k, v, ok := strings.Cut(spec, "=")
			if ok {
				filter.Labels[k] = v
			}
		}
	}
	if limit := strings.TrimSpace(r.URL.Query().Get("limit")); limit != "" {
		if n, err := strconv.Atoi(limit); err == nil && n > 0 {
			filter.Limit = n
		}
	}

	runs, err := db.ListRuns(filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query runs: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs, "count": len(runs)})
}

func handleGetRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database unavailable: "+err.Error())
		return
	}
	defer db.Close()

	run, err := db.GetRun(id)
	if err != nil {
		writeError(w, http.StatusBadRequest, "lookup run: "+err.Error())
		return
	}
	if run == nil {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}

	nodes, _ := db.GetNodeExecutions(run.RunID)
	edges, _ := db.GetEdgeDecisions(run.RunID)
	providers, _ := db.GetProviderSelections(run.RunID)
	diffs, _ := db.GetNodeDiffs(run.RunID)
	children, _ := db.ListRuns(rundb.ListFilter{ParentRunID: run.RunID})
	outputs := gatherOutputRefs(run)

	writeJSON(w, http.StatusOK, map[string]any{
		"run_id":           run.RunID,
		"graph_name":       run.GraphName,
		"goal":             run.Goal,
		"status":           run.Status,
		"started_at":       run.StartedAt,
		"completed_at":     run.CompletedAt,
		"duration_ms":      run.DurationMS,
		"logs_root":        run.LogsRoot,
		"worktree_dir":     run.WorktreeDir,
		"run_branch":       run.RunBranch,
		"repo_path":        run.RepoPath,
		"final_sha":        run.FinalSHA,
		"failure_reason":   run.FailureReason,
		"labels":           run.Labels,
		"inputs":           run.Inputs,
		"warnings":         run.Warnings,
		"node_count":       run.NodeCount,
		"invocation":       run.Invocation,
		"invocation_json":  run.Invocation,
		"config":           run.Config,
		"config_json":      run.Config,
		"parent_run_id":    run.ParentRunID,
		"children":         children,
		"outputs":          outputs,
		"dot_source":       db.GetDotSource(run.RunID),
		"nodes":            nodes,
		"edges":            edges,
		"providers":        providers,
		"provider_summary": summarizeProviders(providers),
		"diffs":            diffs,
	})
}

func handleGetRunOutputs(w http.ResponseWriter, r *http.Request) {
	run, ok := lookupRun(w, r.PathValue("id"))
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"outputs": gatherOutputRefs(run)})
}

func handleDownloadOutput(w http.ResponseWriter, r *http.Request) {
	run, ok := lookupRun(w, r.PathValue("id"))
	if !ok {
		return
	}
	name := filepath.Clean(r.PathValue("name"))
	if name == "." || strings.Contains(name, "..") {
		writeError(w, http.StatusBadRequest, "invalid output name")
		return
	}
	path := filepath.Join(run.LogsRoot, "outputs", name)
	serveFile(w, path)
}

func handleGetNodeAttempts(w http.ResponseWriter, r *http.Request) {
	run, db, ok := lookupRunWithDB(w, r.PathValue("id"))
	if !ok {
		return
	}
	defer db.Close()
	nodeID := r.PathValue("nodeId")
	attempts, err := db.GetNodeAttempts(run.RunID, nodeID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query attempts: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"run_id": run.RunID, "node_id": nodeID, "attempts": attempts, "count": len(attempts),
	})
}

func handleGetNodeTurns(w http.ResponseWriter, r *http.Request) {
	run, db, ok := lookupRunWithDB(w, r.PathValue("id"))
	if !ok {
		return
	}
	defer db.Close()
	nodeID := r.PathValue("nodeId")
	attempt := queryPositiveInt(r, "attempt")

	result := map[string]any{"run_id": run.RunID, "node_id": nodeID, "attempt": attempt}
	var artifacts []rundb.NodeArtifactSummary
	var err error
	if attempt > 0 {
		artifacts, err = db.GetNodeArtifactsForAttempt(run.RunID, nodeID, attempt)
	} else {
		artifacts, err = db.GetNodeArtifactsForRunNode(run.RunID, nodeID)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query artifacts: "+err.Error())
		return
	}
	if len(artifacts) > 0 {
		assignArtifactsToTurnsResult(result, artifacts)
		writeJSON(w, http.StatusOK, result)
		return
	}
	if run.LogsRoot != "" {
		stageDir := filepath.Join(run.LogsRoot, nodeID)
		if _, err := os.Stat(stageDir); err == nil {
			readFilesystemTurns(result, stageDir)
			writeJSON(w, http.StatusOK, result)
			return
		}
	}
	writeJSON(w, http.StatusOK, result)
}

func handleGetNodeDiff(w http.ResponseWriter, r *http.Request) {
	run, db, ok := lookupRunWithDB(w, r.PathValue("id"))
	if !ok {
		return
	}
	defer db.Close()
	nodeID := r.PathValue("nodeId")
	diff, err := db.GetNodeDiff(run.RunID, nodeID, queryPositiveInt(r, "attempt"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "query diff: "+err.Error())
		return
	}
	if diff == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"node_id": nodeID,
			"summary": map[string]any{"files_changed": 0, "insertions": 0, "deletions": 0},
			"files":   []any{},
		})
		return
	}
	out := map[string]any{
		"node_id": diff.NodeID, "attempt": diff.Attempt,
		"before_sha": diff.BeforeSHA, "after_sha": diff.AfterSHA,
		"summary": map[string]any{
			"files_changed": diff.FilesChanged,
			"insertions":    diff.Insertions,
			"deletions":     diff.Deletions,
		},
	}
	if run.WorktreeDir != "" {
		if full, err := gitutil.Diff(run.WorktreeDir, diff.BeforeSHA, diff.AfterSHA); err == nil {
			out["diff"] = full
		}
		if files, err := gitDiffFileList(run.WorktreeDir, diff.BeforeSHA, diff.AfterSHA); err == nil {
			out["files"] = files
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func handleGetRunLog(w http.ResponseWriter, r *http.Request) {
	run, ok := lookupRun(w, r.PathValue("id"))
	if !ok {
		return
	}
	logPath := filepath.Join(run.LogsRoot, "run.log")
	node := r.URL.Query().Get("node")
	source := r.URL.Query().Get("source")
	event := r.URL.Query().Get("event")
	tail := queryPositiveInt(r, "tail")
	var since time.Time
	if s := strings.TrimSpace(r.URL.Query().Get("since")); s != "" {
		since, _ = time.Parse(time.RFC3339, s)
	}
	if r.URL.Query().Get("stream") == "true" {
		streamRunLog(w, r, logPath, node, source, event, since)
		return
	}
	events, err := readFilteredRunLog(logPath, node, source, event, since, tail)
	if err != nil {
		if os.IsNotExist(err) {
			writeJSON(w, http.StatusOK, map[string]any{"events": []any{}, "count": 0, "message": "no run.log found"})
			return
		}
		writeError(w, http.StatusInternalServerError, "read run.log: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events, "count": len(events)})
}

func handleGetRunEvents(w http.ResponseWriter, r *http.Request) {
	run, ok := lookupRun(w, r.PathValue("id"))
	if !ok {
		return
	}
	logPath := filepath.Join(run.LogsRoot, "run.log")
	node := r.URL.Query().Get("node")
	source := r.URL.Query().Get("source")
	event := r.URL.Query().Get("event")
	var since time.Time
	if s := strings.TrimSpace(r.URL.Query().Get("since")); s != "" {
		since, _ = time.Parse(time.RFC3339, s)
	}
	streamRunLog(w, r, logPath, node, source, event, since)
}

func handleBrowseFiles(w http.ResponseWriter, r *http.Request) {
	run, ok := lookupRun(w, r.PathValue("id"))
	if !ok {
		return
	}
	serveDirOrFile(w, run.LogsRoot, r.PathValue("path"))
}

func handleBrowseWorkspace(w http.ResponseWriter, r *http.Request) {
	run, ok := lookupRun(w, r.PathValue("id"))
	if !ok {
		return
	}
	if run.WorktreeDir == "" {
		writeError(w, http.StatusNotFound, "run has no workspace")
		return
	}
	serveDirOrFile(w, run.WorktreeDir, r.PathValue("path"))
}

func handlePolicy(w http.ResponseWriter, _ *http.Request) {
	data, err := policy.LoadEffective("")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "load policy: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, policyResponse(data))
}

func handlePolicyClass(w http.ResponseWriter, r *http.Request) {
	data, err := policy.LoadEffective("")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "load policy: "+err.Error())
		return
	}
	classID := r.PathValue("class")
	cls, ok := data.Classes[classID]
	if !ok {
		writeError(w, http.StatusNotFound, "class not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"class": classID, "policy": cls, "override": data.AppliedOverrides[classID]})
}

func handlePolicyExplain(w http.ResponseWriter, r *http.Request) {
	run, db, ok := lookupRunWithDB(w, r.PathValue("id"))
	if !ok {
		return
	}
	defer db.Close()
	providers, _ := db.GetProviderSelections(run.RunID)
	out := map[string]any{"run_id": run.RunID, "providers": providers}
	for _, name := range []string{"prelaunch_validation.json", "prelaunch.json"} {
		path := filepath.Join(run.LogsRoot, name)
		if data, err := os.ReadFile(path); err == nil {
			var parsed any
			if json.Unmarshal(data, &parsed) == nil {
				out["prelaunch"] = parsed
			} else {
				out["prelaunch_raw"] = string(data)
			}
			out["prelaunch_path"] = path
			break
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func handleAuth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, auth.ListAll(version.Version, auth.DefaultDetectors()))
}

func policyResponse(data *policy.Data) map[string]any {
	return map[string]any{
		"schema_version":    data.SchemaVersion,
		"policy_version":    data.PolicyVersion,
		"classes":           data.Classes,
		"aliases":           data.Aliases,
		"deprecated":        data.Deprecated,
		"applied_overrides": data.AppliedOverrides,
	}
}

func lookupRun(w http.ResponseWriter, id string) (*rundb.RunSummary, bool) {
	db, ok := lookupDB(w)
	if !ok {
		return nil, false
	}
	defer db.Close()
	run, err := db.GetRun(id)
	if err != nil {
		writeError(w, http.StatusBadRequest, "lookup run: "+err.Error())
		return nil, false
	}
	if run == nil {
		writeError(w, http.StatusNotFound, "run not found")
		return nil, false
	}
	return run, true
}

func lookupRunWithDB(w http.ResponseWriter, id string) (*rundb.RunSummary, *rundb.DB, bool) {
	db, ok := lookupDB(w)
	if !ok {
		return nil, nil, false
	}
	run, err := db.GetRun(id)
	if err != nil {
		_ = db.Close()
		writeError(w, http.StatusBadRequest, "lookup run: "+err.Error())
		return nil, nil, false
	}
	if run == nil {
		_ = db.Close()
		writeError(w, http.StatusNotFound, "run not found")
		return nil, nil, false
	}
	return run, db, true
}

func lookupDB(w http.ResponseWriter) (*rundb.DB, bool) {
	db, err := rundb.Open(rundb.DefaultPath())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "database unavailable: "+err.Error())
		return nil, false
	}
	return db, true
}

type outputRef struct {
	Name      string `json:"name"`
	Path      string `json:"path,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
	Found     bool   `json:"found"`
	Source    string `json:"source,omitempty"`
}

func gatherOutputRefs(run *rundb.RunSummary) []outputRef {
	if run == nil || run.LogsRoot == "" {
		return []outputRef{}
	}
	outputsPath := filepath.Join(run.LogsRoot, "outputs.json")
	data, err := os.ReadFile(outputsPath)
	if err != nil {
		return []outputRef{}
	}
	var raw []map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return []outputRef{}
	}
	refs := make([]outputRef, 0, len(raw))
	for _, item := range raw {
		name := firstString(item["name"], item["path"])
		if name == "" {
			continue
		}
		clean := filepath.Clean(name)
		path := filepath.Join(run.LogsRoot, "outputs", clean)
		ref := outputRef{Name: clean, Path: path, Found: false, Source: "collected"}
		if info, err := os.Stat(path); err == nil {
			ref.Found = true
			ref.SizeBytes = info.Size()
		} else if size, ok := numberToInt64(item["size_bytes"]); ok {
			ref.SizeBytes = size
		}
		refs = append(refs, ref)
	}
	return refs
}

func summarizeProviders(in []rundb.ProviderSelectionSummary) []map[string]any {
	seen := map[string]map[string]any{}
	for _, p := range in {
		key := p.Provider + "\x00" + p.Model + "\x00" + p.Backend
		if seen[key] == nil {
			seen[key] = map[string]any{
				"provider": p.Provider,
				"model":    p.Model,
				"backend":  p.Backend,
				"count":    0,
			}
		}
		seen[key]["count"] = seen[key]["count"].(int) + 1
	}
	out := make([]map[string]any, 0, len(seen))
	for _, v := range seen {
		out = append(out, v)
	}
	return out
}

func assignArtifactsToTurnsResult(result map[string]any, artifacts []rundb.NodeArtifactSummary) {
	result["artifacts"] = artifacts
	scripts := []map[string]any{}
	for _, a := range artifacts {
		switch {
		case a.Name == "prompt.md":
			result["prompt"] = string(a.Content)
		case a.Name == "response.md":
			result["response"] = string(a.Content)
		case a.Name == "agent_output.jsonl":
			result["agent_log"] = string(a.Content)
			result["agent_log_format"] = "claude-stream-jsonl"
		case a.Name == "events.ndjson":
			result["agent_log"] = string(a.Content)
			result["agent_log_format"] = "kilroy-events-ndjson"
		case a.Name == "status.json":
			var status map[string]any
			if json.Unmarshal(a.Content, &status) == nil {
				result["status"] = status
			}
		case a.Name == "stdout.log":
			result["stdout"] = string(a.Content)
		case a.Name == "stderr.log":
			result["stderr"] = string(a.Content)
		case a.Name == "tool_timing.json":
			var timing map[string]any
			if json.Unmarshal(a.Content, &timing) == nil {
				result["timing"] = timing
			}
		case a.Name == "tool_invocation.json":
			var inv map[string]any
			if json.Unmarshal(a.Content, &inv) == nil {
				result["tool_invocation"] = inv
			}
		case strings.HasPrefix(a.Name, "tool_script:"):
			scripts = append(scripts, map[string]any{
				"name":         strings.TrimPrefix(a.Name, "tool_script:"),
				"content":      string(a.Content),
				"content_type": a.ContentType,
				"truncated":    a.Truncated,
			})
		}
		if a.Truncated {
			trunc, _ := result["truncated"].([]string)
			result["truncated"] = append(trunc, a.Name)
		}
	}
	if len(scripts) > 0 {
		result["scripts"] = scripts
	}
	result["source"] = "db"
}

func readFilesystemTurns(result map[string]any, stageDir string) {
	readText := func(name, key string) {
		if data, err := os.ReadFile(filepath.Join(stageDir, name)); err == nil {
			result[key] = string(data)
		}
	}
	readText("prompt.md", "prompt")
	readText("response.md", "response")
	readText("stdout.log", "stdout")
	readText("stderr.log", "stderr")
	if data, err := os.ReadFile(filepath.Join(stageDir, "agent_output.jsonl")); err == nil {
		result["agent_log"] = string(data)
		result["agent_log_format"] = "claude-stream-jsonl"
	} else if data, err := os.ReadFile(filepath.Join(stageDir, "events.ndjson")); err == nil {
		result["agent_log"] = string(data)
		result["agent_log_format"] = "kilroy-events-ndjson"
	}
	for _, spec := range []struct{ file, key string }{
		{"status.json", "status"},
		{"tool_timing.json", "timing"},
		{"tool_invocation.json", "tool_invocation"},
	} {
		if data, err := os.ReadFile(filepath.Join(stageDir, spec.file)); err == nil {
			var parsed map[string]any
			if json.Unmarshal(data, &parsed) == nil {
				result[spec.key] = parsed
			}
		}
	}
	result["source"] = "filesystem"
}

func readFilteredRunLog(path, node, source, event string, since time.Time, tail int) ([]json.RawMessage, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var events []json.RawMessage
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := append([]byte{}, scanner.Bytes()...)
		if len(line) == 0 || !matchesLogFilters(line, node, source, event, since) {
			continue
		}
		events = append(events, json.RawMessage(line))
	}
	if tail > 0 && len(events) > tail {
		events = events[len(events)-tail:]
	}
	return events, scanner.Err()
}

func matchesLogFilters(line []byte, node, source, event string, since time.Time) bool {
	if node == "" && source == "" && event == "" && since.IsZero() {
		return true
	}
	var ev struct {
		Ts     string `json:"ts"`
		Source string `json:"source"`
		Node   string `json:"node"`
		Event  string `json:"event"`
	}
	if err := json.Unmarshal(line, &ev); err != nil {
		return false
	}
	if node != "" && ev.Node != node {
		return false
	}
	if source != "" && ev.Source != source {
		return false
	}
	if event != "" && !strings.HasPrefix(ev.Event, event) {
		return false
	}
	if !since.IsZero() {
		if t, err := time.Parse("2006-01-02T15:04:05.000Z", ev.Ts); err == nil && t.Before(since) {
			return false
		}
	}
	return true
}

func streamRunLog(w http.ResponseWriter, r *http.Request, logPath, node, source, event string, since time.Time) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	f, err := os.Open(logPath)
	if err != nil {
		fmt.Fprintf(w, "event: error\ndata: %q\n\n", err.Error())
		flusher.Flush()
		return
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 || !matchesLogFilters(line, node, source, event, since) {
			continue
		}
		fmt.Fprintf(w, "data: %s\n\n", line)
	}
	offset, _ := f.Seek(0, 1)
	_ = f.Close()
	flusher.Flush()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if next := tailNewLogEvents(w, flusher, logPath, offset, node, source, event, since); next > offset {
				offset = next
			}
		}
	}
}

func tailNewLogEvents(w http.ResponseWriter, flusher http.Flusher, logPath string, offset int64, node, source, event string, since time.Time) int64 {
	f, err := os.Open(logPath)
	if err != nil {
		return offset
	}
	defer f.Close()
	if _, err := f.Seek(offset, 0); err != nil {
		return offset
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	wrote := false
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 || !matchesLogFilters(line, node, source, event, since) {
			continue
		}
		fmt.Fprintf(w, "data: %s\n\n", line)
		wrote = true
	}
	next, _ := f.Seek(0, 1)
	if wrote {
		flusher.Flush()
	}
	return next
}

type fileEntry struct {
	Name       string    `json:"name"`
	Size       int64     `json:"size"`
	IsDir      bool      `json:"is_dir"`
	ModifiedAt time.Time `json:"modified_at"`
}

func serveDirOrFile(w http.ResponseWriter, root, subpath string) {
	if strings.TrimSpace(root) == "" {
		writeError(w, http.StatusNotFound, "root is unavailable")
		return
	}
	clean := filepath.Clean("/" + subpath)
	if strings.Contains(clean, "..") {
		writeError(w, http.StatusBadRequest, "invalid path")
		return
	}
	target := filepath.Join(root, clean)
	absRoot, _ := filepath.Abs(root)
	absTarget, _ := filepath.Abs(target)
	if absTarget != absRoot && !strings.HasPrefix(absTarget, absRoot+string(os.PathSeparator)) {
		writeError(w, http.StatusBadRequest, "path traversal denied")
		return
	}
	info, err := os.Stat(target)
	if err != nil {
		writeError(w, http.StatusNotFound, "not found: "+clean)
		return
	}
	if !info.IsDir() {
		serveFile(w, target)
		return
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read directory: "+err.Error())
		return
	}
	files := make([]fileEntry, 0, len(entries))
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, fileEntry{Name: e.Name(), Size: info.Size(), IsDir: e.IsDir(), ModifiedAt: info.ModTime().UTC()})
	}
	writeJSON(w, http.StatusOK, map[string]any{"path": clean, "files": files, "count": len(files)})
}

func serveFile(w http.ResponseWriter, path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	w.Header().Set("Content-Type", contentTypeForPath(path))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func contentTypeForPath(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		return "application/json"
	case ".md":
		return "text/markdown; charset=utf-8"
	case ".txt", ".log", ".dot":
		return "text/plain; charset=utf-8"
	case ".html":
		return "text/html; charset=utf-8"
	case ".yaml", ".yml":
		return "text/yaml; charset=utf-8"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".svg":
		return "image/svg+xml"
	default:
		return "application/octet-stream"
	}
}

func uiHandler() http.Handler {
	webDir := resolveWebDir()
	fs := http.FileServer(http.Dir(webDir))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" {
			http.ServeFile(w, r, filepath.Join(webDir, "index.html"))
			return
		}
		if _, err := os.Stat(filepath.Join(webDir, filepath.Clean(path))); err == nil {
			fs.ServeHTTP(w, r)
			return
		}
		http.ServeFile(w, r, filepath.Join(webDir, "index.html"))
	})
}

func resolveWebDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return filepath.Join("ui", "web")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "web"))
}

type diffFileEntry struct {
	Path       string `json:"path"`
	Status     string `json:"status"`
	Insertions int    `json:"insertions"`
	Deletions  int    `json:"deletions"`
}

func gitDiffFileList(dir, fromSHA, toSHA string) ([]diffFileEntry, error) {
	raw, err := gitutil.DiffFileList(dir, fromSHA, toSHA)
	if err != nil {
		return nil, err
	}
	var entries []diffFileEntry
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 3)
		if len(parts) < 3 {
			continue
		}
		ins, _ := strconv.Atoi(parts[0])
		del, _ := strconv.Atoi(parts[1])
		status := "modified"
		if ins > 0 && del == 0 {
			status = "added"
		}
		entries = append(entries, diffFileEntry{Path: parts[2], Status: status, Insertions: ins, Deletions: del})
	}
	return entries, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func queryPositiveInt(r *http.Request, key string) int {
	if raw := strings.TrimSpace(r.URL.Query().Get(key)); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return 0
}

func firstString(values ...any) string {
	for _, v := range values {
		if s := strings.TrimSpace(fmt.Sprint(v)); s != "" && s != "<nil>" {
			return s
		}
	}
	return ""
}

func numberToInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}

func csrfProtect(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			origin := r.Header.Get("Origin")
			if origin != "" {
				u, err := url.Parse(origin)
				if err != nil {
					writeError(w, http.StatusForbidden, "invalid Origin header")
					return
				}
				host := u.Hostname()
				if host != "localhost" && host != "127.0.0.1" && host != "::1" {
					writeError(w, http.StatusForbidden, "cross-origin request blocked")
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}
