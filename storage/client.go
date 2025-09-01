/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/storage"
	"github.com/PlakarKorp/plakar/network"
)

type Store struct {
	config     storage.Configuration
	Repository string
	location   *url.URL
}

func init() {
	storage.Register("http", 0, NewStore)
	storage.Register("https", 0, NewStore)
}

func NewStore(ctx context.Context, proto string, storeConfig map[string]string) (storage.Store, error) {
	location, err := url.Parse(storeConfig["location"])
	if err != nil {
		return nil, fmt.Errorf("invalid URL %q: %w", storeConfig["location"], err)
	}

	return &Store{
		location: location,
	}, nil
}

func (s *Store) Location(ctx context.Context) (string, error) {
	return s.location.String(), nil
}

func (s *Store) sendRequest(method string, requestType string, payload any) (*http.Response, error) {
	requestBody, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	u := *s.location
	u.Path = path.Join(u.Path, requestType)
	req, err := http.NewRequest(method, u.String(), bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

func (s *Store) Create(ctx context.Context, config []byte) error {
	return nil
}

func (s *Store) Open(ctx context.Context) ([]byte, error) {
	r, err := s.sendRequest("GET", "/", network.ReqOpen{})
	if err != nil {
		return nil, err
	}

	var resOpen network.ResOpen
	if err := json.NewDecoder(r.Body).Decode(&resOpen); err != nil {
		return nil, err
	}
	if resOpen.Err != "" {
		return nil, fmt.Errorf("%s", resOpen.Err)
	}
	return resOpen.Configuration, nil
}

func (s *Store) Close(ctx context.Context) error {
	return nil
}

func (s *Store) Mode(ctx context.Context) (storage.Mode, error) {
	return storage.ModeRead | storage.ModeWrite, nil
}

func (s *Store) Size(ctx context.Context) (int64, error) {
	return -1, nil
}

// states
func (s *Store) GetStates(ctx context.Context) ([]objects.MAC, error) {
	r, err := s.sendRequest("GET", "/states", network.ReqGetStates{})
	if err != nil {
		return nil, err
	}

	var resGetStates network.ResGetStates
	if err := json.NewDecoder(r.Body).Decode(&resGetStates); err != nil {
		return nil, err
	}
	if resGetStates.Err != "" {
		return nil, fmt.Errorf("%s", resGetStates.Err)
	}

	return resGetStates.MACs, nil
}

func (s *Store) PutState(ctx context.Context, MAC objects.MAC, rd io.Reader) (int64, error) {
	data, err := io.ReadAll(rd)
	if err != nil {
		return 0, err
	}

	r, err := s.sendRequest("PUT", "/state", network.ReqPutState{
		MAC:  MAC,
		Data: data,
	})
	if err != nil {
		return 0, err
	}

	var resPutState network.ResPutState
	if err := json.NewDecoder(r.Body).Decode(&resPutState); err != nil {
		return 0, err
	}
	if resPutState.Err != "" {
		return 0, fmt.Errorf("%s", resPutState.Err)
	}
	return int64(len(data)), nil
}

func (s *Store) GetState(ctx context.Context, MAC objects.MAC) (io.ReadCloser, error) {
	r, err := s.sendRequest("GET", "/state", network.ReqGetState{
		MAC: MAC,
	})
	if err != nil {
		return nil, err
	}

	var resGetState network.ResGetState
	if err := json.NewDecoder(r.Body).Decode(&resGetState); err != nil {
		return nil, err
	}
	if resGetState.Err != "" {
		return nil, fmt.Errorf("%s", resGetState.Err)
	}
	return io.NopCloser(bytes.NewBuffer(resGetState.Data)), nil
}

func (s *Store) DeleteState(ctx context.Context, MAC objects.MAC) error {
	r, err := s.sendRequest("DELETE", "/state", network.ReqDeleteState{
		MAC: MAC,
	})
	if err != nil {
		return err
	}

	var resDeleteState network.ResDeleteState
	if err := json.NewDecoder(r.Body).Decode(&resDeleteState); err != nil {
		return err
	}
	if resDeleteState.Err != "" {
		return fmt.Errorf("%s", resDeleteState.Err)
	}
	return nil
}

// packfiles
func (s *Store) GetPackfiles(ctx context.Context) ([]objects.MAC, error) {
	r, err := s.sendRequest("GET", "/packfiles", network.ReqGetPackfiles{})
	if err != nil {
		return nil, err
	}

	var resGetPackfiles network.ResGetPackfiles
	if err := json.NewDecoder(r.Body).Decode(&resGetPackfiles); err != nil {
		return nil, err
	}
	if resGetPackfiles.Err != "" {
		return nil, fmt.Errorf("%s", resGetPackfiles.Err)
	}

	return resGetPackfiles.MACs, nil
}

func (s *Store) PutPackfile(ctx context.Context, MAC objects.MAC, rd io.Reader) (int64, error) {
	data, err := io.ReadAll(rd)
	if err != nil {
		return 0, err
	}
	r, err := s.sendRequest("PUT", "/packfile", network.ReqPutPackfile{
		MAC:  MAC,
		Data: data,
	})
	if err != nil {
		return 0, err
	}

	var resPutPackfile network.ResPutPackfile
	if err := json.NewDecoder(r.Body).Decode(&resPutPackfile); err != nil {
		return 0, err
	}
	if resPutPackfile.Err != "" {
		return 0, fmt.Errorf("%s", resPutPackfile.Err)
	}
	return int64(len(data)), nil
}

func (s *Store) GetPackfile(ctx context.Context, MAC objects.MAC) (io.ReadCloser, error) {
	r, err := s.sendRequest("GET", "/packfile", network.ReqGetPackfile{
		MAC: MAC,
	})
	if err != nil {
		return nil, err
	}

	var resGetPackfile network.ResGetPackfile
	if err := json.NewDecoder(r.Body).Decode(&resGetPackfile); err != nil {
		return nil, err
	}
	if resGetPackfile.Err != "" {
		return nil, fmt.Errorf("%s", resGetPackfile.Err)
	}
	return io.NopCloser(bytes.NewBuffer(resGetPackfile.Data)), nil
}

func (s *Store) GetPackfileBlob(ctx context.Context, MAC objects.MAC, offset uint64, length uint32) (io.ReadCloser, error) {
	r, err := s.sendRequest("GET", "/packfile/blob", network.ReqGetPackfileBlob{
		MAC:    MAC,
		Offset: offset,
		Length: length,
	})
	if err != nil {
		return nil, err
	}

	var resGetPackfileBlob network.ResGetPackfileBlob
	if err := json.NewDecoder(r.Body).Decode(&resGetPackfileBlob); err != nil {
		return nil, err
	}
	if resGetPackfileBlob.Err != "" {
		return nil, fmt.Errorf("%s", resGetPackfileBlob.Err)
	}
	return io.NopCloser(bytes.NewBuffer(resGetPackfileBlob.Data)), nil
}

func (s *Store) DeletePackfile(ctx context.Context, MAC objects.MAC) error {
	r, err := s.sendRequest("DELETE", "/packfile", network.ReqDeletePackfile{
		MAC: MAC,
	})
	if err != nil {
		return err
	}

	var resDeletePackfile network.ResDeletePackfile
	if err := json.NewDecoder(r.Body).Decode(&resDeletePackfile); err != nil {
		return err
	}
	if resDeletePackfile.Err != "" {
		return fmt.Errorf("%s", resDeletePackfile.Err)
	}
	return nil
}

/* Locks */
func (s *Store) GetLocks(ctx context.Context) ([]objects.MAC, error) {
	r, err := s.sendRequest("GET", "/locks", &network.ReqGetLocks{})
	if err != nil {
		return []objects.MAC{}, err
	}

	var res network.ResGetLocks
	if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
		return []objects.MAC{}, err
	}
	if res.Err != "" {
		return []objects.MAC{}, fmt.Errorf("%s", res.Err)
	}
	return res.Locks, nil
}

func (s *Store) PutLock(ctx context.Context, lockID objects.MAC, rd io.Reader) (int64, error) {
	data, err := io.ReadAll(rd)
	if err != nil {
		return 0, err
	}

	req := network.ReqPutLock{
		Mac:  lockID,
		Data: data,
	}
	r, err := s.sendRequest("PUT", "/lock", &req)
	if err != nil {
		return 0, err
	}

	var res network.ResPutLock
	if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
		return 0, err
	}
	if res.Err != "" {
		return 0, fmt.Errorf("%s", res.Err)
	}
	return int64(len(data)), nil
}

func (s *Store) GetLock(ctx context.Context, lockID objects.MAC) (io.ReadCloser, error) {
	req := network.ReqGetLock{
		Mac: lockID,
	}
	r, err := s.sendRequest("GET", "/lock", &req)
	if err != nil {
		return nil, err
	}

	var res network.ResGetLock
	if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
		return nil, err
	}

	if res.Err != "" {
		return nil, fmt.Errorf("%s", res.Err)
	}

	return io.NopCloser(bytes.NewReader(res.Data)), nil
}

func (s *Store) DeleteLock(ctx context.Context, lockID objects.MAC) error {
	req := network.ReqDeleteLock{
		Mac: lockID,
	}
	r, err := s.sendRequest("DELETE", "/lock", &req)
	if err != nil {
		return err
	}

	var res network.ResDeleteLock
	if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
		return err
	}

	if res.Err != "" {
		return fmt.Errorf("%s", res.Err)
	}
	return nil
}
