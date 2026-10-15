/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package proxy

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httputil"
)

func (s *Server) mistralDecodeServer(state *mistralDecodeState) *Server {
	// Each request has its own completion state. Shared proxies must keep their transports.
	clone := s.Clone()
	clone.logger = s.logger
	clone.decoderProxy = mistralDecodeProxy(s.decoderProxy, state)
	clone.dataParallelProxies = make(map[string]http.Handler, len(s.dataParallelProxies))
	for endpoint, handler := range s.dataParallelProxies {
		clone.dataParallelProxies[endpoint] = mistralDecodeProxy(handler, state)
	}
	return clone
}

func mistralDecodeProxy(handler http.Handler, state *mistralDecodeState) http.Handler {
	proxy, ok := handler.(*httputil.ReverseProxy)
	if !ok {
		return handler
	}
	clone := *proxy
	transport := proxy.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	clone.Transport = &mistralDecodeTransport{base: transport, state: state}
	return &clone
}

type mistralDecodeTransport struct {
	base  http.RoundTripper
	state *mistralDecodeState
}

func (t *mistralDecodeTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request.Header.Set("Accept-Encoding", "identity")
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return response, err
	}
	reader, err := nativeResponseBody(response)
	if err != nil {
		_ = response.Body.Close()
		return nil, err
	}
	response.Body = &mistralDecodeBody{reader: reader, source: response.Body, state: t.state}
	response.Header.Del("Content-Encoding")
	response.Header.Del("Content-Length")
	response.ContentLength = -1
	return response, nil
}

type mistralDecodeBody struct {
	reader io.Reader
	source io.ReadCloser
	state  *mistralDecodeState
}

func (b *mistralDecodeBody) Read(p []byte) (int, error) {
	// Native terminal events can arrive before the engine closes the HTTP stream.
	if b.state.terminal {
		return 0, io.EOF
	}
	return b.reader.Read(p)
}

func (b *mistralDecodeBody) Close() error {
	if reader, ok := b.reader.(*gzip.Reader); ok {
		_ = reader.Close()
	}
	return b.source.Close()
}
