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
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
)

func TestMistralMultipartBoundsAndSingleSpool(t *testing.T) {
	for _, maxParts := range []int{4, 2} {
		t.Run(strconv.Itoa(maxParts), func(t *testing.T) {
			tmp := t.TempDir()
			t.Setenv("TMPDIR", tmp)
			var buf bytes.Buffer
			writer := multipart.NewWriter(&buf)
			meta, _ := writer.CreateFormField("completion_request")
			_, _ = io.WriteString(meta, `{"prompt":[1],"max_tokens":128}`)
			for _, name := range []string{"image_0", "image_1", "image_2"} {
				part, _ := writer.CreateFormFile(name, name+".bin")
				_, _ = part.Write([]byte("bytes"))
			}
			_ = writer.Close()
			req := httptest.NewRequest(http.MethodPost, "/v3/completions", bytes.NewReader(buf.Bytes()))
			req.Header.Set("Content-Type", writer.FormDataContentType())
			limits := defaultMistralLimits()
			limits.MaxParts = maxParts
			envelope, err := readMistralRequest(req, reqcommon.APITypeMistralV3, limits)
			if maxParts == 2 {
				if err == nil {
					envelope.close()
					t.Fatal("too many parts accepted")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				files, _ := os.ReadDir(tmp)
				if len(files) != 1 {
					t.Fatalf("spool files=%d", len(files))
				}
				envelope.close()
			}
			files, _ := os.ReadDir(tmp)
			if len(files) != 0 {
				t.Fatalf("leaked files=%v", files)
			}
		})
	}
}

func TestMistralMetadataLimit(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v2/completions", strings.NewReader(`{"prompt":[] ,"padding":"`+strings.Repeat("x", 128)+`"}`))
	limits := defaultMistralLimits()
	limits.MaxMetadataBytes = 32
	envelope, err := readMistralRequest(req, reqcommon.APITypeMistralV2, limits)
	if err == nil {
		envelope.close()
		t.Fatal("oversized metadata accepted")
	}
}

func TestMistralMultipartHeaderWhitespaceBound(t *testing.T) {
	for _, suffix := range []string{"", " \t", strings.Repeat("\t", 128)} {
		for _, size := range []int{128, 32 << 10} {
			t.Run(fmt.Sprintf("%d/%d", len(suffix), size), func(t *testing.T) {
				body := "--sample" + suffix + "\r\nContent-Disposition: form-data; name=\"completion_request\"\r\nX-Padding: " + strings.Repeat("x", size) + "\r\n\r\n{\"prompt\":[1],\"max_tokens\":128}\r\n--sample--\r\n"
				request := httptest.NewRequest(http.MethodPost, "/v3/completions", strings.NewReader(body))
				request.Header.Set("Content-Type", "multipart/form-data; boundary=sample")
				envelope, err := readMistralRequest(request, reqcommon.APITypeMistralV3, defaultMistralLimits())
				if envelope != nil {
					defer envelope.close()
				}
				if size > mistralMaxHeaderBytes && err == nil {
					t.Fatal("oversized multipart headers accepted")
				}
				if size < mistralMaxHeaderBytes && err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
