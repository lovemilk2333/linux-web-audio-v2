package main

import (
	"context"
	"errors"
	"log"
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
	// 2.5ms * 400 = 1s
	BufferRate uint `arg:"--buffer-rate,--buf" help:"opus buffer rate, which means duration = 2.5ms * this" default:"400"`
}

func check_args() error {
	if !strings.HasPrefix(args.BasePath, "/") {
		return errors.New("invalid base path")
	}

	return nil
}

func main() {
	defer logger.Sync()

	arg.MustParse(&args)
	err := check_args()
	if err != nil {
		log.Panicln(err)
	}

	service, err := core.NewService(logger, args.BufferRate)
	if err != nil {
		logger.Fatal("cannot create/init service", zap.Error(err))
	}

	r := gin.Default()
	router := r.Group(args.BasePath)
	router.GET("/stream", service.HandleWebsocket)

	srv := &http.Server{
		Addr:    args.Listen,
		Handler: r.Handler(),
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server cashed", zap.Error(err))
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
		logger.Warn("server shutdown error", zap.Error(err))
	}

	logger.Info("server exited")
}
