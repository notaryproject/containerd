/*
   Copyright The containerd Authors.

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

package docker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	artifactspec "github.com/oras-project/artifacts-spec/specs-go/v1"
	"github.com/pkg/errors"
)

type dockerDiscoverer struct {
	*dockerBase
}

func (r dockerDiscoverer) Discover(ctx context.Context, desc ocispec.Descriptor, artifactType string) ([]artifactspec.Descriptor, error) {
	ctx = log.WithLogger(ctx, log.G(ctx).WithField("digest", desc.Digest))

	hosts := r.filterHosts(HostCapabilityDiscover)
	if len(hosts) == 0 {
		return nil, errors.Wrap(errdefs.ErrNotFound, "no discover hosts")
	}

	ctx, err := ContextWithRepositoryScope(ctx, r.refspec, false)
	if err != nil {
		return nil, err
	}

	v := url.Values{}
	v.Set("artifactType", artifactType)
	query := "?" + v.Encode()

	var firstErr error
	for _, originalHost := range r.hosts {
		host := originalHost
		host.Path = strings.TrimSuffix(host.Path, "/v2") + "/oras/artifacts/v1"

		req := r.request(host, http.MethodGet, "manifests", desc.Digest.String(), "referrers")
		req.path += query
		if err := req.addNamespace(r.refspec.Hostname()); err != nil {
			return nil, err
		}

		refs, err := r.discover(ctx, req)
		if err != nil {
			// Store the error for referencing later
			if firstErr == nil {
				firstErr = err
			}
			continue // try another host
		}

		return refs, nil
	}

	return nil, firstErr
}

func (r dockerDiscoverer) discover(ctx context.Context, req *request) ([]artifactspec.Descriptor, error) {
	resp, err := req.doWithRetries(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var registryErr Errors
		if err := json.NewDecoder(resp.Body).Decode(&registryErr); err != nil || registryErr.Len() < 1 {
			return nil, errors.Errorf("unexpected status code %v: %v", req.String(), resp.Status)
		}
		return nil, errors.Errorf("unexpected status code %v: %s - Server message: %s", req.String(), resp.Status, registryErr.Error())
	}

	result := struct {
		References []artifactspec.Descriptor `json:"references"`
	}{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result.References, nil
}
