// -*- Mode: Go; indent-tabs-mode: t -*-
//
// Copyright (C) 2018-2021 IOTech Ltd
//
// SPDX-License-Identifier: Apache-2.0

package driver

import (
	"context"
	"fmt"
	"strings"
	"testing"

	sdkModel "github.com/edgexfoundry/device-sdk-go/v3/pkg/models"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/clients/logger"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/semaphore"
)

func init() {
	driver = new(Driver)
	driver.Logger = logger.NewMockClient()
}

func TestLockAddressWithAddressCountLimit(t *testing.T) {
    address := "/dev/USB0tty"
    driver.addressMap = map[string]*semaphore.Weighted{}
    sem := semaphore.NewWeighted(int64(concurrentCommandLimit))
    // Acquire all permits to simulate the limit being reached
    err := sem.Acquire(context.Background(), int64(concurrentCommandLimit))
    if err != nil {
        t.Fatalf("Failed to acquire all permits for setup: %v", err)
    }
    driver.addressMap[address] = sem

    err = driver.lockAddress(address)

    if err == nil || !strings.Contains(err.Error(), "High-frequency command execution") {
        t.Errorf("Unexpected result, it should return semaphore acquire error, %v", err)
    }
}

func TestLockAddressWithAddressCountUnderLimit(t *testing.T) {
    address := "/dev/USB0tty"
    driver.addressMap = map[string]*semaphore.Weighted{}
    sem := semaphore.NewWeighted(int64(concurrentCommandLimit))
    // Acquire one less than the limit
    err := sem.Acquire(context.Background(), int64(concurrentCommandLimit-1))
    if err != nil {
        t.Fatalf("Failed to acquire permits for setup: %v", err)
    }
    driver.addressMap[address] = sem

    err = driver.lockAddress(address)

    if err != nil {
        t.Errorf("Unexpected result, address should be locked successfully, %v", err)
    }
}

func TestDriver_createDeviceClient(t *testing.T) {
	mockLogger := logger.NewMockClient()
	type fields struct {
		Logger              logger.LoggingClient
		AsyncCh             chan<- *sdkModel.AsyncValues
		addressMap          map[string]*semaphore.Weighted
		workingAddressCount map[string]int
		stopped             bool
		clientMap           map[string]DeviceClient
	}
	type args struct {
		info *ConnectionInfo
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    DeviceClient
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "OK - reuse modbus-tcp client",
			fields: fields{
				Logger:              mockLogger,
				addressMap:          map[string]*semaphore.Weighted{},
				workingAddressCount: make(map[string]int),
				clientMap: map[string]DeviceClient{
					"modbus-tcp:172.0.0.1:502": &ModbusClient{},
				},
			},
			args: args{
				info: &ConnectionInfo{
					Protocol:    ProtocolTCP,
					Address:     "172.0.0.1",
					Port:        502,
					BaudRate:    9600,
					DataBits:    8,
					StopBits:    1,
					Parity:      "N",
					UnitID:      1,
					Timeout:     1000,
					IdleTimeout: 5000,
				},
			},
			want:    &ModbusClient{},
			wantErr: assert.NoError,
		},
		{
			name: "OK - reuse modbus-rtu client",
			fields: fields{
				Logger:              mockLogger,
				addressMap:          map[string]*semaphore.Weighted{},
				workingAddressCount: make(map[string]int),
				clientMap: map[string]DeviceClient{
					"modbus-rtu:172.0.0.1:502:9600:8:1:N": &ModbusClient{},
				},
			},
			args: args{
				info: &ConnectionInfo{
					Protocol:    ProtocolRTU,
					Address:     "172.0.0.1",
					Port:        502,
					BaudRate:    9600,
					DataBits:    8,
					StopBits:    1,
					Parity:      "N",
					UnitID:      1,
					Timeout:     1000,
					IdleTimeout: 5000,
				},
			},
			want:    &ModbusClient{},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Driver{
				Logger:              tt.fields.Logger,
				AsyncCh:             tt.fields.AsyncCh,
				addressMap:          tt.fields.addressMap,
				// workingAddressCount: tt.fields.workingAddressCount,
				stopped:             tt.fields.stopped,
				clientMap:           tt.fields.clientMap,
			}
			got, err := d.createDeviceClient(tt.args.info, false)
			if !tt.wantErr(t, err, fmt.Sprintf("createDeviceClient(%v)", tt.args.info)) {
				return
			}
			assert.Equalf(t, tt.want, got, "createDeviceClient(%v)", tt.args.info)
		})
	}
}
