package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/taskstore"
)

type APIServer struct {
	cfg        *config.Config
	server     *http.Server
	validator  *certValidator
	taskStore  *taskstore.Store
	listenAddr string
	jobCtx     context.Context
	jobCancel  context.CancelFunc
	wg         sync.WaitGroup
}

func New(cfg *config.Config) (*APIServer, error) {
	if cfg == nil {
		return nil, fmt.Errorf("configuration is not loaded")
	}
	srvCfg := cfg.Server
	if srvCfg.ListenAddress == "" {
		srvCfg.ListenAddress = "0.0.0.0"
	}
	if srvCfg.ListenPort == 0 {
		return nil, fmt.Errorf("server.listen_port must be configured")
	}
	if srvCfg.TLSCertFile == "" || srvCfg.TLSKeyFile == "" {
		return nil, fmt.Errorf("server TLS certificate and key must be configured")
	}

	validator, err := newCertValidator(cfg)
	if err != nil {
		return nil, err
	}

	taskStore, err := taskstore.New(cfg.Server.TaskStorePath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialise task store: %w", err)
	}

	tlsCert, err := tls.LoadX509KeyPair(srvCfg.TLSCertFile, srvCfg.TLSKeyFile)
	if err != nil {
		taskStore.Close()
		return nil, fmt.Errorf("failed to load server TLS keypair: %w", err)
	}

	mux := http.NewServeMux()

	apiServer := &APIServer{
		cfg:        cfg,
		validator:  validator,
		taskStore:  taskStore,
		listenAddr: fmt.Sprintf("%s:%d", srvCfg.ListenAddress, srvCfg.ListenPort),
		jobCtx:     context.Background(),
	}

	mux.Handle("/api/v1/table-diff", apiServer.authenticated(http.HandlerFunc(apiServer.handleTableDiff)))
	mux.Handle("/api/v1/table-rerun", apiServer.authenticated(http.HandlerFunc(apiServer.handleTableRerun)))
	mux.Handle("/api/v1/table-repair", apiServer.authenticated(http.HandlerFunc(apiServer.handleTableRepair)))
	mux.Handle("/api/v1/spock-diff", apiServer.authenticated(http.HandlerFunc(apiServer.handleSpockDiff)))
	mux.Handle("/api/v1/schema-diff", apiServer.authenticated(http.HandlerFunc(apiServer.handleSchemaDiff)))
	mux.Handle("/api/v1/repset-diff", apiServer.authenticated(http.HandlerFunc(apiServer.handleRepsetDiff)))
	mux.Handle("/api/v1/tasks/", apiServer.authenticated(http.HandlerFunc(apiServer.handleTaskStatus)))

	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{tlsCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    validator.clientCAPool,
	}

	apiServer.server = &http.Server{
		Addr:              apiServer.listenAddr,
		Handler:           loggingMiddleware(mux),
		TLSConfig:         tlsConfig,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		IdleTimeout:       120 * time.Second,
		BaseContext: func(_ net.Listener) context.Context {
			return context.Background()
		},
	}

	return apiServer, nil
}

func (s *APIServer) Run(ctx context.Context) error {
	if s == nil || s.server == nil {
		return fmt.Errorf("api server is not initialized")
	}

	runCtx, cancel := context.WithCancel(ctx)
	s.jobCtx = runCtx
	s.jobCancel = cancel
	defer func() {
		cancel()
		s.wg.Wait()
		if s.taskStore != nil {
			if err := s.taskStore.Close(); err != nil {
				logger.Warn("failed to close task store: %v", err)
			}
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("API server listening on https://%s", s.listenAddr)
		if err := s.server.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := s.server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("failed to shutdown API server: %w", err)
		}
		return nil
	case err := <-errCh:
		return err
	}
}

func (s *APIServer) enqueueTask(taskID string, run func(context.Context) error) error {
	if s == nil {
		return fmt.Errorf("api server unavailable")
	}
	if s.taskStore == nil {
		return fmt.Errorf("task store unavailable")
	}
	if s.jobCtx == nil {
		return fmt.Errorf("api server is not running")
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ctx, cancel := context.WithCancel(s.jobCtx)
		defer cancel()
		if err := run(ctx); err != nil {
			logger.Error("task %s failed: %v", taskID, err)
		}
	}()
	return nil
}

type clientInfo struct {
	cert *x509.Certificate
	role string
}

type clientContextKey struct{}

func (s *APIServer) authenticated(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			writeError(w, http.StatusUnauthorized, "client certificate required")
			return
		}
		clientCert := r.TLS.PeerCertificates[0]
		role, err := s.validator.Validate(clientCert)
		if err != nil {
			logger.Warn("client certificate validation failed: %v", err)
			writeError(w, http.StatusUnauthorized, err.Error())
			return
		}
		info := clientInfo{cert: clientCert, role: role}
		ctx := context.WithValue(r.Context(), clientContextKey{}, info)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func getClientInfo(ctx context.Context) (clientInfo, bool) {
	if ctx == nil {
		return clientInfo{}, false
	}
	info, ok := ctx.Value(clientContextKey{}).(clientInfo)
	return info, ok
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if payload == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		logger.Warn("failed to write JSON response: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	if message == "" {
		message = http.StatusText(status)
	}
	writeJSON(w, status, map[string]string{"error": message})
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		logger.Debug("%s %s completed in %s", r.Method, r.URL.Path, time.Since(start))
	})
}
