package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"webrtc-file-downloader/internal/config"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/pion/webrtc/v4"
)

const (
	// DataChannelLabel must match the label created by the client before it
	// creates its SDP offer.
	DataChannelLabel = "file-transfer"

	// DataChannelMessageSizeBytes stays within Pion's non-detached OnMessage
	// receive limit. Binary file messages include an 8-byte sequence header.
	DataChannelMessageSizeBytes = 16 * 1024

	// BinaryChunkHeaderBytes stores a uint64 big-endian chunk sequence number.
	BinaryChunkHeaderBytes = 8

	// DataChunkSizeBytes is the maximum file payload in one binary message.
	DataChunkSizeBytes = DataChannelMessageSizeBytes - BinaryChunkHeaderBytes

	// DataBatchSizeChunks controls how many chunks are accepted before the
	// server sends a batch acknowledgement to the client. One full batch is
	// approximately 512 KiB.
	DataBatchSizeChunks = 32

	// MaxControlMessageBytes protects the JSON control-message decoder.
	MaxControlMessageBytes = DataChannelMessageSizeBytes

	// MaxSignalBodyBytes protects the HTTP signaling endpoint.
	MaxSignalBodyBytes = 2 * 1024 * 1024

	// MaxTransferSizeBytes is a safety limit, not a WebRTC protocol limit.
	MaxTransferSizeBytes int64 = 2 * 1024 * 1024 * 1024
)

var (
	errSessionNotFound      = errors.New("session not found")
	errDataChannelNotReady  = errors.New("data channel is not open")
	errTransferAlreadyBusy  = errors.New("another transfer is pending or active")
	errUnexpectedTransferID = errors.New("unexpected transfer ID")
)

type application struct {
	log      *slog.Logger
	sessions *sessionStore
	webrtc   webrtc.Configuration
}

type sessionStore struct {
	mu       sync.RWMutex
	sessions map[string]*peerSession
}

type peerSession struct {
	id        string
	clientID  string
	createdAt time.Time
	log       *slog.Logger
	store     *sessionStore
	pc        *webrtc.PeerConnection
	downloads string

	mu                sync.RWMutex
	dc                *webrtc.DataChannel
	pendingTransferID string
	activeTransfer    *incomingTransfer
	closed            bool

	sendMu sync.Mutex
}

type incomingTransfer struct {
	mu sync.Mutex

	id             string
	name           string
	expectedSize   int64
	expectedSHA256 string
	receivedBytes  int64
	nextSequence   uint64
	chunksInBatch  int

	tempPath  string
	finalPath string
	file      *os.File
	hasher    hash.Hash
	closed    bool
}

type signalRequest struct {
	ClientID string                    `json:"client_id"`
	Offer    webrtc.SessionDescription `json:"offer"`
}

type signalResponse struct {
	SessionID string                    `json:"session_id"`
	Answer    webrtc.SessionDescription `json:"answer"`
}

type fileRequestResponse struct {
	SessionID  string `json:"session_id"`
	TransferID string `json:"transfer_id"`
	Status     string `json:"status"`
}

type sessionResponse struct {
	SessionID       string    `json:"session_id"`
	ClientID        string    `json:"client_id"`
	CreatedAt       time.Time `json:"created_at"`
	ConnectionState string    `json:"connection_state"`
	DataChannelOpen bool      `json:"data_channel_open"`
	TransferState   string    `json:"transfer_state"`
}

type controlEnvelope struct {
	Type string `json:"type"`
}

type requestFileControl struct {
	Type           string `json:"type"`
	TransferID     string `json:"transfer_id"`
	ChunkSizeBytes int    `json:"chunk_size_bytes"`
	BatchSize      int    `json:"batch_size_chunks"`
}

type fileStartControl struct {
	Type       string `json:"type"`
	TransferID string `json:"transfer_id"`
	Name       string `json:"name"`
	SizeBytes  int64  `json:"size_bytes"`
	SHA256     string `json:"sha256,omitempty"`
}

type fileEndControl struct {
	Type       string `json:"type"`
	TransferID string `json:"transfer_id"`
}

type batchAckControl struct {
	Type          string `json:"type"`
	TransferID    string `json:"transfer_id"`
	NextSequence  uint64 `json:"next_sequence"`
	ReceivedBytes int64  `json:"received_bytes"`
}

type fileReceivedControl struct {
	Type       string `json:"type"`
	TransferID string `json:"transfer_id"`
	SizeBytes  int64  `json:"size_bytes"`
	SHA256     string `json:"sha256"`
	FileName   string `json:"file_name"`
}

type errorControl struct {
	Type       string `json:"type"`
	TransferID string `json:"transfer_id,omitempty"`
	Message    string `json:"message"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := config.LoadServer(); err != nil {
		logger.Error("load configuration", "error", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(config.AppConfig.DownloadPath, 0o750); err != nil {
		logger.Error("create download directory", "error", err, "path", config.AppConfig.DownloadPath)
		os.Exit(1)
	}

	app := &application{
		log:      logger,
		sessions: newSessionStore(),
		webrtc: webrtc.Configuration{
			ICEServers: []webrtc.ICEServer{{URLs: config.AppConfig.STUNURLs}},
		},
	}

	server := &http.Server{
		Addr:              config.AppConfig.ServerAddress,
		Handler:           app.routes(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       45 * time.Second,
		WriteTimeout:      45 * time.Second,
		IdleTimeout:       90 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("signaling server started", "address", config.AppConfig.ServerAddress, "download_path", config.AppConfig.DownloadPath)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = server.Shutdown(shutdownCtx)
	app.sessions.closeAll()
}

func (app *application) routes() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(40 * time.Second))
	r.Use(app.cors)

	r.Get("/healthz", app.health)

	r.Route("/api/v1/webrtc", func(r chi.Router) {
		r.Post("/sessions", app.createSession)
		r.Get("/sessions/{sessionID}", app.getSession)
		r.Delete("/sessions/{sessionID}", app.deleteSession)
		r.Post("/sessions/{sessionID}/files/request", app.requestFile)
	})

	return r
}

func (app *application) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", config.AppConfig.AllowedOrigin)
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Request-ID")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (app *application) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (app *application) createSession(w http.ResponseWriter, r *http.Request) {
	var request signalRequest
	if err := decodeJSONBody(w, r, &request, MaxSignalBodyBytes); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	request.ClientID = strings.TrimSpace(request.ClientID)
	if request.ClientID == "" {
		writeError(w, http.StatusBadRequest, errors.New("client_id is required"))
		return
	}
	if request.Offer.Type != webrtc.SDPTypeOffer || strings.TrimSpace(request.Offer.SDP) == "" {
		writeError(w, http.StatusBadRequest, errors.New("offer must contain a valid SDP offer"))
		return
	}

	pc, err := webrtc.NewPeerConnection(app.webrtc)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("create peer connection: %w", err))
		return
	}

	session := &peerSession{
		id:        newID(),
		clientID:  request.ClientID,
		createdAt: time.Now().UTC(),
		log:       app.log.With("client_id", request.ClientID),
		store:     app.sessions,
		pc:        pc,
		downloads: config.AppConfig.DownloadPath,
	}
	app.sessions.add(session)
	registered := true

	defer func() {
		if registered {
			return
		}
		app.sessions.remove(session.id)
		_ = pc.Close()
	}()

	session.registerPeerCallbacks()

	if err := pc.SetRemoteDescription(request.Offer); err != nil {
		registered = false
		writeError(w, http.StatusBadRequest, fmt.Errorf("set remote description: %w", err))
		return
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		registered = false
		writeError(w, http.StatusInternalServerError, fmt.Errorf("create answer: %w", err))
		return
	}

	gatheringComplete := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		registered = false
		writeError(w, http.StatusInternalServerError, fmt.Errorf("set local description: %w", err))
		return
	}

	select {
	case <-gatheringComplete:
	case <-r.Context().Done():
		registered = false
		writeError(w, http.StatusGatewayTimeout, errors.New("ICE candidate gathering timed out"))
		return
	}

	localDescription := pc.LocalDescription()
	if localDescription == nil {
		registered = false
		writeError(w, http.StatusInternalServerError, errors.New("local SDP answer is unavailable"))
		return
	}

	writeJSON(w, http.StatusCreated, signalResponse{
		SessionID: session.id,
		Answer:    *localDescription,
	})
}

func (app *application) getSession(w http.ResponseWriter, r *http.Request) {
	session, ok := app.sessions.get(chi.URLParam(r, "sessionID"))
	if !ok {
		writeError(w, http.StatusNotFound, errSessionNotFound)
		return
	}
	writeJSON(w, http.StatusOK, session.snapshot())
}

func (app *application) deleteSession(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "sessionID")
	session, ok := app.sessions.get(id)
	if !ok {
		writeError(w, http.StatusNotFound, errSessionNotFound)
		return
	}

	app.sessions.remove(id)
	session.close()
	w.WriteHeader(http.StatusNoContent)
}

func (app *application) requestFile(w http.ResponseWriter, r *http.Request) {
	session, ok := app.sessions.get(chi.URLParam(r, "sessionID"))
	if !ok {
		writeError(w, http.StatusNotFound, errSessionNotFound)
		return
	}

	transferID, err := session.requestFile()
	if err != nil {
		switch {
		case errors.Is(err, errDataChannelNotReady):
			writeError(w, http.StatusConflict, err)
		case errors.Is(err, errTransferAlreadyBusy):
			writeError(w, http.StatusConflict, err)
		default:
			writeError(w, http.StatusInternalServerError, err)
		}
		return
	}

	writeJSON(w, http.StatusAccepted, fileRequestResponse{
		SessionID:  session.id,
		TransferID: transferID,
		Status:     "requested",
	})
}

func newSessionStore() *sessionStore {
	return &sessionStore{sessions: make(map[string]*peerSession)}
}

func (s *sessionStore) add(session *peerSession) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[session.id] = session
}

func (s *sessionStore) get(id string) (*peerSession, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session, ok := s.sessions[id]
	return session, ok
}

func (s *sessionStore) remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, id)
}

func (s *sessionStore) closeAll() {
	s.mu.Lock()
	sessions := make([]*peerSession, 0, len(s.sessions))
	for _, session := range s.sessions {
		sessions = append(sessions, session)
	}
	s.sessions = make(map[string]*peerSession)
	s.mu.Unlock()

	for _, session := range sessions {
		session.close()
	}
}

func (s *peerSession) registerPeerCallbacks() {
	s.pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		s.log.Info("peer connection state changed", "session_id", s.id, "state", state.String())
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			s.store.remove(s.id)
			s.close()
		}
	})

	s.pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		if dc.Label() != DataChannelLabel {
			s.log.Warn("rejecting unexpected data channel", "session_id", s.id, "label", dc.Label())
			_ = dc.Close()
			return
		}

		s.mu.Lock()
		if s.closed || s.dc != nil {
			s.mu.Unlock()
			_ = dc.Close()
			return
		}
		s.dc = dc
		s.mu.Unlock()

		dc.OnOpen(func() {
			s.log.Info("data channel opened", "session_id", s.id, "label", dc.Label())
		})
		dc.OnClose(func() {
			s.log.Info("data channel closed", "session_id", s.id)
			s.clearDataChannel(dc)
			s.abortActiveTransfer(errors.New("data channel closed"))
		})
		dc.OnError(func(err error) {
			s.log.Error("data channel error", "session_id", s.id, "error", err)
		})
		dc.OnMessage(func(message webrtc.DataChannelMessage) {
			if message.IsString {
				s.handleControlMessage(message.Data)
				return
			}
			s.handleBinaryChunk(message.Data)
		})
	})
}

func (s *peerSession) snapshot() sessionResponse {
	s.mu.RLock()
	defer s.mu.RUnlock()

	transferState := "idle"
	if s.activeTransfer != nil {
		transferState = "receiving"
	} else if s.pendingTransferID != "" {
		transferState = "requested"
	}

	dataChannelOpen := s.dc != nil && s.dc.ReadyState() == webrtc.DataChannelStateOpen
	return sessionResponse{
		SessionID:       s.id,
		ClientID:        s.clientID,
		CreatedAt:       s.createdAt,
		ConnectionState: s.pc.ConnectionState().String(),
		DataChannelOpen: dataChannelOpen,
		TransferState:   transferState,
	}
}

func (s *peerSession) requestFile() (string, error) {
	transferID := newID()

	s.mu.Lock()
	if s.closed || s.dc == nil || s.dc.ReadyState() != webrtc.DataChannelStateOpen {
		s.mu.Unlock()
		return "", errDataChannelNotReady
	}
	if s.pendingTransferID != "" || s.activeTransfer != nil {
		s.mu.Unlock()
		return "", errTransferAlreadyBusy
	}
	s.pendingTransferID = transferID
	s.mu.Unlock()

	message := requestFileControl{
		Type:           "request_file",
		TransferID:     transferID,
		ChunkSizeBytes: DataChunkSizeBytes,
		BatchSize:      DataBatchSizeChunks,
	}
	if err := s.sendControl(message); err != nil {
		s.mu.Lock()
		if s.pendingTransferID == transferID {
			s.pendingTransferID = ""
		}
		s.mu.Unlock()
		return "", fmt.Errorf("send file request: %w", err)
	}

	return transferID, nil
}

func (s *peerSession) handleControlMessage(data []byte) {
	if len(data) == 0 || len(data) > MaxControlMessageBytes {
		s.sendProtocolError("", "invalid control message size")
		return
	}

	var envelope controlEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		s.sendProtocolError("", "invalid control message JSON")
		return
	}

	switch envelope.Type {
	case "file_start":
		var message fileStartControl
		if err := json.Unmarshal(data, &message); err != nil {
			s.sendProtocolError("", "invalid file_start message")
			return
		}
		if err := s.startIncomingTransfer(message); err != nil {
			s.sendProtocolError(message.TransferID, err.Error())
		}

	case "file_end":
		var message fileEndControl
		if err := json.Unmarshal(data, &message); err != nil {
			s.sendProtocolError("", "invalid file_end message")
			return
		}
		if err := s.finishIncomingTransfer(message.TransferID); err != nil {
			s.sendProtocolError(message.TransferID, err.Error())
		}

	case "error":
		var message errorControl
		if err := json.Unmarshal(data, &message); err == nil {
			s.log.Error("client transfer error", "session_id", s.id, "transfer_id", message.TransferID, "message", message.Message)
			s.abortActiveTransfer(errors.New(message.Message))
			s.clearPendingTransfer(message.TransferID)
		}

	default:
		s.sendProtocolError("", "unsupported control message type")
	}
}

func (s *peerSession) startIncomingTransfer(message fileStartControl) error {
	message.TransferID = strings.TrimSpace(message.TransferID)
	message.Name = sanitizeFileName(message.Name)
	message.SHA256 = strings.ToLower(strings.TrimSpace(message.SHA256))

	if message.TransferID == "" {
		return errors.New("transfer_id is required")
	}
	if message.Name == "" {
		return errors.New("file name is required")
	}
	if message.SizeBytes < 0 || message.SizeBytes > MaxTransferSizeBytes {
		return fmt.Errorf("file size must be between 0 and %d bytes", MaxTransferSizeBytes)
	}
	if message.SHA256 != "" {
		decoded, err := hex.DecodeString(message.SHA256)
		if err != nil || len(decoded) != sha256.Size {
			return errors.New("sha256 must be a 64-character hexadecimal value")
		}
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("session is closed")
	}
	if s.activeTransfer != nil {
		s.mu.Unlock()
		return errTransferAlreadyBusy
	}
	if s.pendingTransferID == "" || s.pendingTransferID != message.TransferID {
		s.mu.Unlock()
		return errUnexpectedTransferID
	}

	baseName := fmt.Sprintf("%s_%s_%s", s.id, message.TransferID, message.Name)
	finalPath := filepath.Join(s.downloads, baseName)
	tempPath := finalPath + ".part"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("create destination file: %w", err)
	}

	s.activeTransfer = &incomingTransfer{
		id:             message.TransferID,
		name:           message.Name,
		expectedSize:   message.SizeBytes,
		expectedSHA256: message.SHA256,
		tempPath:       tempPath,
		finalPath:      finalPath,
		file:           file,
		hasher:         sha256.New(),
	}
	s.pendingTransferID = ""
	s.mu.Unlock()

	s.log.Info("file transfer started", "session_id", s.id, "transfer_id", message.TransferID, "name", message.Name, "size_bytes", message.SizeBytes)
	return nil
}

func (s *peerSession) handleBinaryChunk(data []byte) {
	if len(data) < BinaryChunkHeaderBytes || len(data) > BinaryChunkHeaderBytes+DataChunkSizeBytes {
		s.sendProtocolError("", "binary chunk has an invalid size")
		s.abortActiveTransfer(errors.New("invalid binary chunk size"))
		return
	}

	s.mu.RLock()
	transfer := s.activeTransfer
	s.mu.RUnlock()
	if transfer == nil {
		s.sendProtocolError("", "received binary data without an active transfer")
		return
	}

	sequence := binary.BigEndian.Uint64(data[:BinaryChunkHeaderBytes])
	payload := data[BinaryChunkHeaderBytes:]
	ack, nextSequence, receivedBytes, err := transfer.writeChunk(sequence, payload)
	if err != nil {
		s.sendProtocolError(transfer.id, err.Error())
		s.abortActiveTransfer(err)
		return
	}

	if ack {
		if err := s.sendControl(batchAckControl{
			Type:          "batch_ack",
			TransferID:    transfer.id,
			NextSequence:  nextSequence,
			ReceivedBytes: receivedBytes,
		}); err != nil {
			s.log.Error("send batch acknowledgement", "session_id", s.id, "transfer_id", transfer.id, "error", err)
			s.abortActiveTransfer(err)
		}
	}
}

func (t *incomingTransfer) writeChunk(sequence uint64, payload []byte) (bool, uint64, int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return false, t.nextSequence, t.receivedBytes, errors.New("transfer is already closed")
	}
	if sequence != t.nextSequence {
		return false, t.nextSequence, t.receivedBytes, fmt.Errorf("unexpected chunk sequence: got %d, want %d", sequence, t.nextSequence)
	}
	if t.receivedBytes+int64(len(payload)) > t.expectedSize {
		return false, t.nextSequence, t.receivedBytes, errors.New("received data exceeds declared file size")
	}

	if len(payload) > 0 {
		if _, err := t.file.Write(payload); err != nil {
			return false, t.nextSequence, t.receivedBytes, fmt.Errorf("write destination file: %w", err)
		}
		if _, err := t.hasher.Write(payload); err != nil {
			return false, t.nextSequence, t.receivedBytes, fmt.Errorf("update checksum: %w", err)
		}
	}

	t.receivedBytes += int64(len(payload))
	t.nextSequence++
	t.chunksInBatch++

	shouldAck := t.chunksInBatch >= DataBatchSizeChunks || t.receivedBytes == t.expectedSize
	if shouldAck {
		t.chunksInBatch = 0
	}
	return shouldAck, t.nextSequence, t.receivedBytes, nil
}

func (s *peerSession) finishIncomingTransfer(transferID string) error {
	s.mu.Lock()
	transfer := s.activeTransfer
	if transfer == nil || transfer.id != transferID {
		s.mu.Unlock()
		return errUnexpectedTransferID
	}
	s.activeTransfer = nil
	s.mu.Unlock()

	size, checksum, fileName, err := transfer.finish()
	if err != nil {
		transfer.abort()
		return err
	}

	if err := s.sendControl(fileReceivedControl{
		Type:       "file_received",
		TransferID: transferID,
		SizeBytes:  size,
		SHA256:     checksum,
		FileName:   fileName,
	}); err != nil {
		s.log.Error("send file completion acknowledgement", "session_id", s.id, "transfer_id", transferID, "error", err)
	}

	s.log.Info("file transfer completed", "session_id", s.id, "transfer_id", transferID, "file", fileName, "size_bytes", size, "sha256", checksum)
	return nil
}

func (t *incomingTransfer) finish() (int64, string, string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return 0, "", "", errors.New("transfer is already closed")
	}
	if t.receivedBytes != t.expectedSize {
		return 0, "", "", fmt.Errorf("incomplete file: received %d of %d bytes", t.receivedBytes, t.expectedSize)
	}

	checksum := hex.EncodeToString(t.hasher.Sum(nil))
	if t.expectedSHA256 != "" && checksum != t.expectedSHA256 {
		return 0, "", "", fmt.Errorf("checksum mismatch: got %s, want %s", checksum, t.expectedSHA256)
	}

	if err := t.file.Sync(); err != nil {
		return 0, "", "", fmt.Errorf("sync destination file: %w", err)
	}
	if err := t.file.Close(); err != nil {
		return 0, "", "", fmt.Errorf("close destination file: %w", err)
	}
	if err := os.Rename(t.tempPath, t.finalPath); err != nil {
		return 0, "", "", fmt.Errorf("commit destination file: %w", err)
	}

	t.closed = true
	return t.receivedBytes, checksum, filepath.Base(t.finalPath), nil
}

func (t *incomingTransfer) abort() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	t.closed = true
	_ = t.file.Close()
	_ = os.Remove(t.tempPath)
}

func (s *peerSession) abortActiveTransfer(reason error) {
	s.mu.Lock()
	transfer := s.activeTransfer
	s.activeTransfer = nil
	s.mu.Unlock()
	if transfer == nil {
		return
	}
	transfer.abort()
	s.log.Warn("file transfer aborted", "session_id", s.id, "transfer_id", transfer.id, "reason", reason)
}

func (s *peerSession) clearPendingTransfer(transferID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if transferID == "" || s.pendingTransferID == transferID {
		s.pendingTransferID = ""
	}
}

func (s *peerSession) clearDataChannel(dc *webrtc.DataChannel) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dc == dc {
		s.dc = nil
		s.pendingTransferID = ""
	}
}

func (s *peerSession) sendProtocolError(transferID, message string) {
	if err := s.sendControl(errorControl{Type: "error", TransferID: transferID, Message: message}); err != nil {
		s.log.Error("send protocol error", "session_id", s.id, "transfer_id", transferID, "error", err)
	}
}

func (s *peerSession) sendControl(value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}

	s.mu.RLock()
	dc := s.dc
	closed := s.closed
	s.mu.RUnlock()
	if closed || dc == nil || dc.ReadyState() != webrtc.DataChannelStateOpen {
		return errDataChannelNotReady
	}

	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	return dc.SendText(string(payload))
}

func (s *peerSession) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	dc := s.dc
	s.dc = nil
	transfer := s.activeTransfer
	s.activeTransfer = nil
	s.pendingTransferID = ""
	s.mu.Unlock()

	if transfer != nil {
		transfer.abort()
	}
	if dc != nil {
		_ = dc.Close()
	}
	_ = s.pc.Close()
}

func sanitizeFileName(name string) string {
	name = strings.TrimSpace(name)
	name = filepath.Base(name)
	name = strings.ReplaceAll(name, "\x00", "")
	if name == "." || name == string(filepath.Separator) {
		return ""
	}
	return name
}

func newID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		panic(fmt.Sprintf("generate random ID: %v", err))
	}
	return hex.EncodeToString(bytes[:])
}

func decodeJSONBody(w http.ResponseWriter, r *http.Request, destination any, maxBytes int64) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode JSON body: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("JSON body must contain exactly one object")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		slog.Error("encode JSON response", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, errorResponse{Error: err.Error()})
}
