package proxy

import (
	"fmt"
	"log"
	"sync"
)

type Service struct {
	proxys map[string]Proxy
}

func New(config Config) (*Service, error) {
	s := new(Service)
	s.proxys = make(map[string]Proxy)

	for _, cfg := range config.HTTPProxys {
		h, err := NewHTTP(cfg)
		if err != nil {
			return nil, err
		}
		if s.proxys[h.Name()] != nil {
			return nil, fmt.Errorf("duplicate proxy: %q", h.Name())
		}
		s.proxys[h.Name()] = h
	}

	return s, nil
}

func (s *Service) Run() {
	var wg sync.WaitGroup
	wg.Add(len(s.proxys))

	for k := range s.proxys {
		proxy := s.proxys[k]
		go func() {
			defer wg.Done()

			if err := proxy.Run(); err != nil {
				log.Printf("Error running proxy %q: %v", proxy.Name(), err)
			}
		}()
	}

	wg.Wait()
}

func (s *Service) Stop() {
	for _, v := range s.proxys {
		v.Stop()
	}
}

type Proxy interface {
	Name() string
	Run() error
	Stop() error
}
