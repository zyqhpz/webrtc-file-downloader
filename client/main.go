package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"webrtc-file-downloader/internal/config"

	"github.com/pion/webrtc/v4"
)

const (
	// DataChannelLabel must match the signaling server.
	DataChannelLabel = "file-transfer"

	// Pion's non-detached OnMessage handler supports messages up to 16 KiB.
	DataChannelMessageSizeBytes = 16 * 1024

	// BinaryChunkHeaderBytes stores a uint64 big-endian chunk sequence number.
	BinaryChunkHeaderBytes = 8

	// DataChunkSizeBytes is the maximum file payload in one binary message.
	DataChunkSizeBytes = DataChannelMessageSizeBytes - BinaryChunkHeaderBytes

	// DataBatchSizeChunks must match the server's acknowledgement interval.
	DataBatchSizeChunks = 32

	MaxControlMessageBytes = DataChannelMessageSizeBytes
	MaxSignalBodyBytes     = 2 * 1024 * 1024
	MaxTransferSizeBytes   = int64(2 * 1024 * 1024 * 1024)

	SignalRequestTimeout        = 45 * time.Second
	ConnectionStartupTimeout    = 45 * time.Second
	DisconnectedGracePeriod     = 20 * time.Second
	BatchAcknowledgementTimeout = 30 * time.Second
	TransferCompletionTimeout   = 30 * time.Second
	ReconnectInitialDelay       = 2 * time.Second
	ReconnectMaximumDelay       = 30 * time.Second
	ProgressLogIntervalBytes    = int64(10 * 1024 * 1024)
)

var (
	errSessionClosed       = errors.New("peer session is closed")
	errDataChannelNotReady = errors.New("data channel is not open")
	errTransferBusy        = errors.New("another file transfer is already active")
)

type agent struct {
	log        *slog.Logger
	httpClient *http.Client
}

type peerSession struct {
	ctx    context.Context
	cancel context.CancelFunc

	log *slog.Logger
	pc  *webrtc.PeerConnection
	dc  *webrtc.DataChannel

	mu        sync.RWMutex
	sessionID string
	active    *outgoingTransfer
	closed    bool

	sendMu sync.Mutex

	openOnce sync.Once
	openCh   chan struct{}

	terminalOnce sync.Once
	terminalCh   chan error

	closeOnce sync.Once
}

type outgoingTransfer struct {
	id string

	ackCh      chan batchAckControl
	receivedCh chan fileReceivedControl
	errCh      chan error
}

type signalRequest struct {
	ClientID string                    `json:"client_id"`
	Offer    webrtc.SessionDescription `json:"offer"`
}

type signalResponse struct {
	SessionID string                    `json:"session_id"`
	Answer    webrtc.SessionDescription `json:"answer"`
}

type errorResponse struct {
	Error string `json:"error"`
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

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := config.LoadClient(); err != nil {
		logger.Error("load configuration", "error", err)
		os.Exit(1)
	}

	runCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client := &agent{
		log: logger.With("client_id", config.AppConfig.ClientID),
		httpClient: &http.Client{
			Timeout: SignalRequestTimeout,
		},
	}

	logger.Info(
		"file transfer client starting",
		"client_id", config.AppConfig.ClientID,
		"signaling_url", config.AppConfig.SignalingURL,
		"local_file_path", config.AppConfig.LocalFilePath,
		"local_file_name", config.AppConfig.LocalFileName,
	)

	if err := client.run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("client stopped", "error", err)
		os.Exit(1)
	}

	logger.Info("client stopped")
}

func (a *agent) run(ctx context.Context) error {
	backoff := ReconnectInitialDelay

	for {
		established, err := a.runConnection(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if established {
			backoff = ReconnectInitialDelay
		}

		a.log.Warn("peer connection ended; reconnecting", "error", err, "retry_in", backoff)

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}

		backoff *= 2
		if backoff > ReconnectMaximumDelay {
			backoff = ReconnectMaximumDelay
		}
	}
}

func (a *agent) runConnection(parent context.Context) (bool, error) {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: config.AppConfig.STUNURLs}},
	})
	if err != nil {
		return false, fmt.Errorf("create peer connection: %w", err)
	}

	ordered := true
	dc, err := pc.CreateDataChannel(DataChannelLabel, &webrtc.DataChannelInit{
		Ordered: &ordered,
	})
	if err != nil {
		_ = pc.Close()
		return false, fmt.Errorf("create data channel: %w", err)
	}

	sessionCtx, cancel := context.WithCancel(parent)
	session := &peerSession{
		ctx:        sessionCtx,
		cancel:     cancel,
		log:        a.log,
		pc:         pc,
		dc:         dc,
		openCh:     make(chan struct{}),
		terminalCh: make(chan error, 1),
	}
	session.registerCallbacks()

	defer func() {
		sessionID := session.getSessionID()
		if sessionID != "" {
			a.deleteSignalingSession(sessionID)
		}
		session.close()
	}()

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return false, fmt.Errorf("create SDP offer: %w", err)
	}

	gatheringComplete := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		return false, fmt.Errorf("set local SDP offer: %w", err)
	}

	gatherCtx, gatherCancel := context.WithTimeout(sessionCtx, ConnectionStartupTimeout)
	defer gatherCancel()

	select {
	case <-gatheringComplete:
	case <-gatherCtx.Done():
		return false, fmt.Errorf("wait for ICE candidate gathering: %w", gatherCtx.Err())
	}

	localDescription := pc.LocalDescription()
	if localDescription == nil {
		return false, errors.New("local SDP offer is unavailable")
	}

	response, err := a.createSignalingSession(sessionCtx, *localDescription)
	if err != nil {
		return false, err
	}
	session.setSessionID(response.SessionID)

	if err := pc.SetRemoteDescription(response.Answer); err != nil {
		return false, fmt.Errorf("set remote SDP answer: %w", err)
	}

	startupCtx, startupCancel := context.WithTimeout(sessionCtx, ConnectionStartupTimeout)
	defer startupCancel()

	select {
	case <-session.openCh:
		a.log.Info("peer connection established", "session_id", response.SessionID)
	case err := <-session.terminalCh:
		return false, err
	case <-startupCtx.Done():
		return false, fmt.Errorf("wait for data channel to open: %w", startupCtx.Err())
	}

	select {
	case <-parent.Done():
		return true, parent.Err()
	case err := <-session.terminalCh:
		return true, err
	}
}

func (a *agent) createSignalingSession(ctx context.Context, offer webrtc.SessionDescription) (signalResponse, error) {
	payload, err := json.Marshal(signalRequest{
		ClientID: config.AppConfig.ClientID,
		Offer:    offer,
	})
	if err != nil {
		return signalResponse{}, fmt.Errorf("encode signaling request: %w", err)
	}

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		config.AppConfig.SignalingURL+"/api/v1/webrtc/sessions",
		bytes.NewReader(payload),
	)
	if err != nil {
		return signalResponse{}, fmt.Errorf("create signaling request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := a.httpClient.Do(request)
	if err != nil {
		return signalResponse{}, fmt.Errorf("send signaling request: %w", err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(io.LimitReader(response.Body, MaxSignalBodyBytes+1))
	if err != nil {
		return signalResponse{}, fmt.Errorf("read signaling response: %w", err)
	}
	if len(body) > MaxSignalBodyBytes {
		return signalResponse{}, errors.New("signaling response exceeds maximum size")
	}

	if response.StatusCode != http.StatusCreated {
		var apiError errorResponse
		if json.Unmarshal(body, &apiError) == nil && strings.TrimSpace(apiError.Error) != "" {
			return signalResponse{}, fmt.Errorf("signaling server returned %s: %s", response.Status, apiError.Error)
		}
		return signalResponse{}, fmt.Errorf("signaling server returned %s", response.Status)
	}

	var result signalResponse
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return signalResponse{}, fmt.Errorf("decode signaling response: %w", err)
	}

	if strings.TrimSpace(result.SessionID) == "" {
		return signalResponse{}, errors.New("signaling response is missing session_id")
	}
	if result.Answer.Type != webrtc.SDPTypeAnswer || strings.TrimSpace(result.Answer.SDP) == "" {
		return signalResponse{}, errors.New("signaling response contains an invalid SDP answer")
	}

	return result, nil
}

func (a *agent) deleteSignalingSession(sessionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		config.AppConfig.SignalingURL+"/api/v1/webrtc/sessions/"+url.PathEscape(sessionID),
		nil,
	)
	if err != nil {
		return
	}

	response, err := a.httpClient.Do(request)
	if err != nil {
		return
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 16*1024))
}

func (s *peerSession) registerCallbacks() {
	s.pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		s.log.Info("peer connection state changed", "state", state.String())

		switch state {
		case webrtc.PeerConnectionStateDisconnected:
			go s.failIfStillDisconnected()
		case webrtc.PeerConnectionStateFailed:
			s.fail(fmt.Errorf("peer connection entered %s state", state))
		case webrtc.PeerConnectionStateClosed:
			s.fail(errSessionClosed)
		}
	})

	s.dc.OnOpen(func() {
		s.log.Info("data channel opened", "label", s.dc.Label())
		s.openOnce.Do(func() { close(s.openCh) })
	})

	s.dc.OnClose(func() {
		s.log.Warn("data channel closed")
		s.fail(errors.New("data channel closed"))
	})

	s.dc.OnError(func(err error) {
		s.log.Error("data channel error", "error", err)
		s.fail(fmt.Errorf("data channel error: %w", err))
	})

	s.dc.OnMessage(func(message webrtc.DataChannelMessage) {
		if !message.IsString {
			s.sendProtocolError("", "client accepts control messages only")
			return
		}
		s.handleControlMessage(message.Data)
	})
}

func (s *peerSession) failIfStillDisconnected() {
	timer := time.NewTimer(DisconnectedGracePeriod)
	defer timer.Stop()

	select {
	case <-s.ctx.Done():
		return
	case <-timer.C:
	}

	if s.pc.ConnectionState() == webrtc.PeerConnectionStateDisconnected {
		s.fail(errors.New("peer connection remained disconnected beyond grace period"))
	}
}

func (s *peerSession) fail(err error) {
	s.terminalOnce.Do(func() {
		s.cancel()
		s.failActiveTransfer(err)
		select {
		case s.terminalCh <- err:
		default:
		}
	})
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
	case "request_file":
		var message requestFileControl
		if err := json.Unmarshal(data, &message); err != nil {
			s.sendProtocolError("", "invalid request_file message")
			return
		}
		s.handleFileRequest(message)

	case "batch_ack":
		var message batchAckControl
		if err := json.Unmarshal(data, &message); err != nil {
			s.sendProtocolError("", "invalid batch_ack message")
			return
		}
		s.deliverBatchAcknowledgement(message)

	case "file_received":
		var message fileReceivedControl
		if err := json.Unmarshal(data, &message); err != nil {
			s.sendProtocolError("", "invalid file_received message")
			return
		}
		s.deliverFileReceived(message)

	case "error":
		var message errorControl
		if err := json.Unmarshal(data, &message); err != nil {
			s.log.Error("received invalid error control message")
			return
		}
		s.deliverTransferError(message)

	default:
		s.sendProtocolError("", "unsupported control message type")
	}
}

func (s *peerSession) handleFileRequest(message requestFileControl) {
	message.TransferID = strings.TrimSpace(message.TransferID)

	if message.TransferID == "" {
		s.sendProtocolError("", "transfer_id is required")
		return
	}
	if message.ChunkSizeBytes != DataChunkSizeBytes {
		s.sendProtocolError(
			message.TransferID,
			fmt.Sprintf("unsupported chunk size: got %d, want %d", message.ChunkSizeBytes, DataChunkSizeBytes),
		)
		return
	}
	if message.BatchSize != DataBatchSizeChunks {
		s.sendProtocolError(
			message.TransferID,
			fmt.Sprintf("unsupported batch size: got %d, want %d", message.BatchSize, DataBatchSizeChunks),
		)
		return
	}

	transfer := &outgoingTransfer{
		id:         message.TransferID,
		ackCh:      make(chan batchAckControl, 1),
		receivedCh: make(chan fileReceivedControl, 1),
		errCh:      make(chan error, 1),
	}

	if err := s.reserveTransfer(transfer); err != nil {
		s.sendProtocolError(message.TransferID, err.Error())
		return
	}

	go func() {
		defer s.releaseTransfer(transfer)

		if err := s.sendSelectedFile(message, transfer); err != nil {
			s.log.Error("file transfer failed", "transfer_id", message.TransferID, "error", err)
			if s.ctx.Err() == nil && s.dc.ReadyState() == webrtc.DataChannelStateOpen {
				s.sendProtocolError(message.TransferID, err.Error())
			}
			return
		}

		s.log.Info("file transfer completed", "transfer_id", message.TransferID)
	}()
}

func (s *peerSession) reserveTransfer(transfer *outgoingTransfer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || s.ctx.Err() != nil {
		return errSessionClosed
	}
	if s.active != nil {
		return errTransferBusy
	}
	s.active = transfer
	return nil
}

func (s *peerSession) releaseTransfer(transfer *outgoingTransfer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active == transfer {
		s.active = nil
	}
}

func (s *peerSession) activeTransfer() *outgoingTransfer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}

func (s *peerSession) deliverBatchAcknowledgement(message batchAckControl) {
	transfer := s.activeTransfer()
	if transfer == nil || transfer.id != message.TransferID {
		s.log.Warn("ignoring batch acknowledgement for unknown transfer", "transfer_id", message.TransferID)
		return
	}

	select {
	case transfer.ackCh <- message:
	default:
		s.signalTransferError(transfer, errors.New("duplicate or unexpected batch acknowledgement"))
	}
}

func (s *peerSession) deliverFileReceived(message fileReceivedControl) {
	transfer := s.activeTransfer()
	if transfer == nil || transfer.id != message.TransferID {
		s.log.Warn("ignoring completion acknowledgement for unknown transfer", "transfer_id", message.TransferID)
		return
	}

	select {
	case transfer.receivedCh <- message:
	default:
		s.signalTransferError(transfer, errors.New("duplicate file completion acknowledgement"))
	}
}

func (s *peerSession) deliverTransferError(message errorControl) {
	serverErr := errors.New(strings.TrimSpace(message.Message))
	if serverErr.Error() == "" {
		serverErr = errors.New("server reported an unspecified transfer error")
	}

	transfer := s.activeTransfer()
	if transfer == nil || (message.TransferID != "" && transfer.id != message.TransferID) {
		s.log.Error("server protocol error", "transfer_id", message.TransferID, "error", serverErr)
		return
	}

	s.signalTransferError(transfer, fmt.Errorf("server rejected transfer: %w", serverErr))
}

func (s *peerSession) failActiveTransfer(err error) {
	transfer := s.activeTransfer()
	if transfer != nil {
		s.signalTransferError(transfer, err)
	}
}

func (s *peerSession) signalTransferError(transfer *outgoingTransfer, err error) {
	select {
	case transfer.errCh <- err:
	default:
	}
}

func (s *peerSession) sendSelectedFile(request requestFileControl, transfer *outgoingTransfer) error {
	file, info, canonicalPath, err := selectFileForTransfer(s.ctx)
	if err != nil {
		return err
	}
	defer file.Close()

	if info.Size() > MaxTransferSizeBytes {
		return fmt.Errorf("file size %d exceeds maximum transfer size %d", info.Size(), MaxTransferSizeBytes)
	}

	checksum, err := calculateSHA256(file)
	if err != nil {
		return fmt.Errorf("calculate file checksum: %w", err)
	}

	afterHash, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat file after hashing: %w", err)
	}
	if fileChanged(info, afterHash) {
		return errors.New("file changed while its checksum was being calculated")
	}

	if err := s.sendControl(fileStartControl{
		Type:       "file_start",
		TransferID: request.TransferID,
		Name:       filepath.Base(canonicalPath),
		SizeBytes:  info.Size(),
		SHA256:     checksum,
	}); err != nil {
		return fmt.Errorf("send file_start: %w", err)
	}

	s.log.Info(
		"file transfer started",
		"transfer_id", request.TransferID,
		"file_name", filepath.Base(canonicalPath),
		"size_bytes", info.Size(),
		"sha256", checksum,
	)

	if err := s.sendFileChunks(file, info.Size(), transfer); err != nil {
		return err
	}

	afterSend, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat file after transfer: %w", err)
	}
	if fileChanged(info, afterSend) {
		return errors.New("file changed during transfer")
	}

	if err := s.sendControl(fileEndControl{
		Type:       "file_end",
		TransferID: request.TransferID,
	}); err != nil {
		return fmt.Errorf("send file_end: %w", err)
	}

	completion, err := s.waitForCompletion(transfer)
	if err != nil {
		return err
	}
	if completion.SizeBytes != info.Size() {
		return fmt.Errorf("server confirmed unexpected size: got %d, want %d", completion.SizeBytes, info.Size())
	}
	if !strings.EqualFold(completion.SHA256, checksum) {
		return fmt.Errorf("server confirmed unexpected checksum: got %s, want %s", completion.SHA256, checksum)
	}

	s.log.Info(
		"server verified transferred file",
		"transfer_id", request.TransferID,
		"server_file_name", completion.FileName,
		"size_bytes", completion.SizeBytes,
		"sha256", completion.SHA256,
	)
	return nil
}

func (s *peerSession) sendFileChunks(file *os.File, fileSize int64, transfer *outgoingTransfer) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind file before transfer: %w", err)
	}

	packet := make([]byte, BinaryChunkHeaderBytes+DataChunkSizeBytes)
	var sequence uint64
	var sentBytes int64
	chunksInBatch := 0
	lastProgressLog := int64(0)

	for sentBytes < fileSize {
		remaining := fileSize - sentBytes
		payloadSize := DataChunkSizeBytes
		if remaining < int64(payloadSize) {
			payloadSize = int(remaining)
		}

		payload := packet[BinaryChunkHeaderBytes : BinaryChunkHeaderBytes+payloadSize]
		if _, err := io.ReadFull(file, payload); err != nil {
			return fmt.Errorf("read source file at byte %d: %w", sentBytes, err)
		}

		binary.BigEndian.PutUint64(packet[:BinaryChunkHeaderBytes], sequence)
		if err := s.sendBinary(packet[:BinaryChunkHeaderBytes+payloadSize]); err != nil {
			return fmt.Errorf("send chunk %d: %w", sequence, err)
		}

		sequence++
		sentBytes += int64(payloadSize)
		chunksInBatch++

		if chunksInBatch >= DataBatchSizeChunks || sentBytes == fileSize {
			if err := s.waitForBatchAcknowledgement(transfer, sequence, sentBytes); err != nil {
				return err
			}
			chunksInBatch = 0

			if sentBytes-lastProgressLog >= ProgressLogIntervalBytes || sentBytes == fileSize {
				s.log.Info(
					"file transfer progress",
					"transfer_id", transfer.id,
					"sent_bytes", sentBytes,
					"total_bytes", fileSize,
				)
				lastProgressLog = sentBytes
			}
		}
	}

	return nil
}

func (s *peerSession) waitForBatchAcknowledgement(
	transfer *outgoingTransfer,
	expectedNextSequence uint64,
	expectedReceivedBytes int64,
) error {
	timer := time.NewTimer(BatchAcknowledgementTimeout)
	defer timer.Stop()

	select {
	case acknowledgement := <-transfer.ackCh:
		if acknowledgement.NextSequence != expectedNextSequence {
			return fmt.Errorf(
				"invalid batch acknowledgement sequence: got %d, want %d",
				acknowledgement.NextSequence,
				expectedNextSequence,
			)
		}
		if acknowledgement.ReceivedBytes != expectedReceivedBytes {
			return fmt.Errorf(
				"invalid batch acknowledgement byte count: got %d, want %d",
				acknowledgement.ReceivedBytes,
				expectedReceivedBytes,
			)
		}
		return nil

	case err := <-transfer.errCh:
		return err

	case <-s.ctx.Done():
		return s.ctx.Err()

	case <-timer.C:
		return fmt.Errorf(
			"timed out waiting for batch acknowledgement at sequence %d",
			expectedNextSequence,
		)
	}
}

func (s *peerSession) waitForCompletion(transfer *outgoingTransfer) (fileReceivedControl, error) {
	timer := time.NewTimer(TransferCompletionTimeout)
	defer timer.Stop()

	select {
	case completion := <-transfer.receivedCh:
		return completion, nil
	case err := <-transfer.errCh:
		return fileReceivedControl{}, err
	case <-s.ctx.Done():
		return fileReceivedControl{}, s.ctx.Err()
	case <-timer.C:
		return fileReceivedControl{}, errors.New("timed out waiting for server file verification")
	}
}

func (s *peerSession) sendControl(value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(payload) > MaxControlMessageBytes {
		return errors.New("control message exceeds maximum size")
	}

	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	if s.dc.ReadyState() != webrtc.DataChannelStateOpen {
		return errDataChannelNotReady
	}
	return s.dc.SendText(string(payload))
}

func (s *peerSession) sendBinary(payload []byte) error {
	if len(payload) == 0 || len(payload) > DataChannelMessageSizeBytes {
		return fmt.Errorf("binary message size %d is invalid", len(payload))
	}

	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	if s.dc.ReadyState() != webrtc.DataChannelStateOpen {
		return errDataChannelNotReady
	}
	return s.dc.Send(payload)
}

func (s *peerSession) sendProtocolError(transferID, message string) {
	if err := s.sendControl(errorControl{
		Type:       "error",
		TransferID: transferID,
		Message:    message,
	}); err != nil {
		s.log.Error("send protocol error", "transfer_id", transferID, "error", err)
	}
}

func (s *peerSession) setSessionID(sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessionID = sessionID
}

func (s *peerSession) getSessionID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessionID
}

func (s *peerSession) close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		transfer := s.active
		s.active = nil
		s.mu.Unlock()

		s.cancel()
		if transfer != nil {
			s.signalTransferError(transfer, errSessionClosed)
		}
		_ = s.dc.Close()
		_ = s.pc.Close()
	})
}

// selectFileForTransfer is intentionally owned by the client. The server only
// requests a transfer and never specifies a path or filename.
func selectFileForTransfer(ctx context.Context) (*os.File, os.FileInfo, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, "", err
	}

	selectedPath := filepath.Join(config.AppConfig.LocalFilePath, config.AppConfig.LocalFileName)
	canonicalPath, err := filepath.EvalSymlinks(selectedPath)
	if err != nil {
		return nil, nil, "", fmt.Errorf("resolve client-selected file: %w", err)
	}
	canonicalPath = filepath.Clean(canonicalPath)

	file, err := os.Open(canonicalPath)
	if err != nil {
		return nil, nil, "", fmt.Errorf("open client-selected file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, "", fmt.Errorf("stat client-selected file: %w", err)
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, nil, "", errors.New("client-selected source is not a regular file")
	}
	if info.Size() < 0 || info.Size() > MaxTransferSizeBytes {
		_ = file.Close()
		return nil, nil, "", fmt.Errorf(
			"client-selected file size must be between 0 and %d bytes",
			MaxTransferSizeBytes,
		)
	}

	return file, info, canonicalPath, nil
}

func calculateSHA256(file *os.File) (string, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}

	hasher := sha256.New()
	buffer := make([]byte, 1024*1024)
	if _, err := io.CopyBuffer(hasher, file, buffer); err != nil {
		return "", err
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func fileChanged(before, after os.FileInfo) bool {
	return before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime())
}
