package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alexflint/go-arg"
	"github.com/gin-gonic/gin"
	"github.com/lovemilk2333/linux-web-audio-v2/core"
	"go.uber.org/zap"
)

var logger = zap.Must(zap.NewProduction()).Sugar()

var args struct {
	BasePath string `arg:"-b,--base" help:"base url path which starts with '/'" default:"/backend/v2/"`
	Listen   string `arg:"-l,--listen" help:"listen at" default:":8643"`
	// 10ms * 100 = 1s
	BufferRate     uint          `arg:"--buffer-rate,--buf" help:"opus buffer rate, which means duration = 10ms * this" default:"100"`
	AudioThreshold uint          `arg:"--audio-threshold,--threshold" help:"minimum audio frames before sending a packet" default:"4"`
	IdleThreshold  time.Duration `arg:"--idle-threshold" help:"stop recording after this idle duration; 0 disables automatic stopping" default:"30s"`
	AllowMic       bool          `arg:"--allow-mic" help:"allow the WebUI to capture microphone sources"`
}

func check_args() error {
	if !strings.HasPrefix(args.BasePath, "/") {
		return errors.New("invalid base path")
	}

	return nil
}

// webUIConnectURL is the address a user can paste into the WebUI stream field.
// A wildcard listen address is shown as localhost so the URL is usable as-is.
func webUIConnectURL(listen, basePath string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		host, port = listen, ""
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		host = "localhost"
	}
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		host = "[" + host + "]"
	}

	path := "/" + strings.Trim(basePath, "/")
	if path == "/" {
		path = ""
	}
	if port != "" {
		return fmt.Sprintf("http://%s:%s%s/stream", host, port, path)
	}
	return fmt.Sprintf("http://%s%s/stream", host, path)
}

func main() {
	defer logger.Sync()
	if os.Getenv("GIN_MODE") == "" {
		gin.SetMode(gin.ReleaseMode)
	}

	arg.MustParse(&args)
	err := check_args()
	if err != nil {
		log.Panicln(err)
	}

	service, err := core.NewServiceWithOptions(logger, core.ServiceOptions{
		BufferRate:     args.BufferRate,
		AudioThreshold: args.AudioThreshold,
		IdleThreshold:  args.IdleThreshold,
		AllowMic:       args.AllowMic,
	})
	if err != nil {
		logger.Fatalw("cannot create/init service", "error", err)
	}

	r := gin.Default()
	r.Use(func(c *gin.Context) {
		// The WebUI is often opened from another origin, such as the Vite dev server.
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Headers", "Accept, Content-Type")
		c.Header("Access-Control-Allow-Methods", "GET, OPTIONS")
		if c.Request.Method == http.MethodOptions {
			c.Status(http.StatusNoContent)
			c.Abort()
			return
		}
		c.Next()
	})
	router := r.Group(args.BasePath)
	router.GET("/config", service.HandleConfig)
	router.GET("/stream", service.HandleWebsocket)

	listener, err := net.Listen("tcp", args.Listen)
	if err != nil {
		logger.Fatalw("cannot listen", "address", args.Listen, "error", err)
	}
	// The bound address carries the real port when --listen used port 0.
	connectURL := webUIConnectURL(listener.Addr().String(), args.BasePath)
	logger.Infof("connect %s from WebUI", connectURL)

	srv := &http.Server{
		Handler: r.Handler(),
	}

	go func() {
		if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
			logger.Fatalw("server crashed", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("server shuting down...")

	service.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Warnw("server shutdown error", "error", err)
	}

	logger.Info("server exited")
}
