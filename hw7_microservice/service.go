package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"strings"
	"time"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type BizHundler struct {
	BizServer
}

func (*BizHundler) Check(ctx context.Context, in *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (*BizHundler) Add(ctx context.Context, in *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (*BizHundler) Test(ctx context.Context, in *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

type ACL map[string][]string

type AdminHandler struct {
	ctx context.Context
	AdminServer

	broadcastLogCh   chan *Event
	addLogListenerCh chan chan *Event
	logListeners     []chan *Event

	broadcastStatCh   chan *Stat
	addStatListenerCh chan chan *Stat
	statListeners     []chan *Stat
}

func (s *AdminHandler) Logging(nothing *Nothing, srv Admin_LoggingServer) error {
	ch := make(chan *Event, 0)
	s.addLogListenerCh <- ch

	for {
		select {
		case event := <-ch:
			err := srv.Send(event)
			if err != nil {
				return err
			}
		case <-s.ctx.Done():
			return nil
		}
	}
}

func (s *AdminHandler) Statistics(interval *StatInterval, srv Admin_StatisticsServer) error {
	ch := make(chan *Stat, 0)
	s.addStatListenerCh <- ch

	ticker := time.NewTicker(time.Second * time.Duration(interval.IntervalSeconds))
	sum := &Stat{
		ByMethod:   make(map[string]uint64),
		ByConsumer: make(map[string]uint64),
	}

	for {
		select {
		case stat := <-ch:
			for k, v := range stat.ByMethod {
				sum.ByMethod[k] += v
			}
			for k, v := range stat.ByConsumer {
				sum.ByConsumer[k] += v
			}
		case <-ticker.C:
			err := srv.Send(sum)
			if err != nil {
				return err
			}
			sum = &Stat{
				ByMethod:   make(map[string]uint64),
				ByConsumer: make(map[string]uint64),
			}
		case <-s.ctx.Done():
			return nil
		}
	}
}

type Server struct {
	acl ACL
	AdminHandler
	BizHundler
}

func StartMyMicroservice(ctx context.Context, listenAddr, ACLData string) error {

	srv := &Server{}
	srv.ctx = ctx

	if err := json.Unmarshal([]byte(ACLData), &srv.acl); err != nil {
		return err
	}

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return errors.New(fmt.Sprintf("cant listen port %s", err))
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(srv.unaryInterceptor),
		grpc.StreamInterceptor(srv.streamInterceptor),
	)

	RegisterBizServer(grpcServer, srv)
	RegisterAdminServer(grpcServer, srv)

	go func() {
		err = grpcServer.Serve(lis)
		if err != nil {
			log.Fatal(err)
		}

	}()

	go func() {
		<-ctx.Done()
		grpcServer.Stop()
	}()

	srv.broadcastLogCh = make(chan *Event, 0)
	srv.addLogListenerCh = make(chan chan *Event, 0)
	go func() {
		for {
			select {
			case ch := <-srv.addLogListenerCh:
				srv.logListeners = append(srv.logListeners, ch)
			case event := <-srv.broadcastLogCh:
				for _, ch := range srv.logListeners {
					ch <- event
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	srv.broadcastStatCh = make(chan *Stat, 0)
	srv.addStatListenerCh = make(chan chan *Stat, 0)
	go func() {
		for {
			select {
			case ch := <-srv.addStatListenerCh:
				srv.statListeners = append(srv.statListeners, ch)
			case stat := <-srv.broadcastStatCh:
				for _, ch := range srv.statListeners {
					ch <- stat
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

func (s *Server) checks(ctx context.Context, fullMethod string) error {
	meta, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Errorf(codes.Unauthenticated, "matadata is empty")
	}

	consumer, ok := meta["consumer"]
	if !ok || len(consumer) != 1 {
		return status.Errorf(codes.Unauthenticated, "matadata is empty")
	}

	allowedPaths, ok := s.acl[consumer[0]]
	if !ok {
		return status.Errorf(codes.Unauthenticated, "no allowed path")
	}

	splittedMethod := strings.Split(fullMethod, "/")
	if len(splittedMethod) != 3 {
		return status.Errorf(codes.Unauthenticated, "wrong method")
	}

	path, method := splittedMethod[1], splittedMethod[2]
	isAllowed := false
	for _, alPath := range allowedPaths {
		splitted := strings.Split(alPath, "/")
		if len(splitted) != 3 {
			continue
		}
		allowedPath, allowedMethod := splitted[1], splitted[2]

		if path != allowedPath {
			continue
		}
		if method == allowedMethod || allowedMethod == "*" {
			isAllowed = true
			break
		}
	}
	if !isAllowed {
		return status.Errorf(codes.Unauthenticated, "method is not allowed")
	}
	return nil
}

func (s *Server) unaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {

	if err := s.checks(ctx, info.FullMethod); err != nil {
		return nil, err
	}

	meta, _ := metadata.FromIncomingContext(ctx)

	s.broadcastLogCh <- &Event{
		Consumer: meta["consumer"][0],
		Method:   info.FullMethod,
		Host:     "127.0.0.1:8083",
	}

	s.broadcastStatCh <- &Stat{
		ByConsumer: map[string]uint64{meta["consumer"][0]: 1},
		ByMethod:   map[string]uint64{info.FullMethod: 1},
	}

	return handler(ctx, req)
}

func (s *Server) streamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {

	if err := s.checks(ss.Context(), info.FullMethod); err != nil {
		return err
	}

	meta, _ := metadata.FromIncomingContext(ss.Context())

	s.broadcastLogCh <- &Event{
		Consumer: meta["consumer"][0],
		Method:   info.FullMethod,
		Host:     "127.0.0.1:8083",
	}

	s.broadcastStatCh <- &Stat{
		ByConsumer: map[string]uint64{meta["consumer"][0]: 1},
		ByMethod:   map[string]uint64{info.FullMethod: 1},
	}

	return handler(srv, ss)
}
