// -*- Mode: Go; indent-tabs-mode: t -*-
//
// Copyright (C) 2018-2023 IOTech Ltd
//
// SPDX-License-Identifier: Apache-2.0

// Package driver is used to execute device-sdk's commands
package driver

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edgexfoundry/device-sdk-go/v3/pkg/interfaces"
	sdkModel "github.com/edgexfoundry/device-sdk-go/v3/pkg/models"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v3/models"
)

var once sync.Once
var driver *Driver

type Driver struct {
	Logger              logger.LoggingClient
	AsyncCh             chan<- *sdkModel.AsyncValues
	addressMutex        sync.RWMutex
	addressMap          map[string]chan bool
	workingAddressCount map[string]*atomic.Int32
	stopped             bool
	clientMutex         sync.RWMutex
	clientMap           map[string]DeviceClient
}

var concurrentCommandLimit int32 = 100

const maxRetries = 5
const retryDelay = time.Millisecond * 80

func (d *Driver) createDeviceClient(info *ConnectionInfo, recreate bool) (c DeviceClient, err error) {
	key := info.String()
	d.clientMutex.RLock()
	c, ok := d.clientMap[key]
	d.clientMutex.RUnlock()
	if ok && !recreate {
		// if the client already exists and we don't need to recreate it, return the existing client
		d.Logger.Debugf("Device client already exists for key: %s, returning existing client", key)
		return
	}

	d.clientMutex.Lock()
	defer d.clientMutex.Unlock()

	// close the old client if it exists and we are recreating
	if c != nil {
		d.Logger.Debugf("Closing existing device client for key: %s", key)
		err = c.CloseConnection()
		if err != nil {
			driver.Logger.Errorf("close device client failed. err:%v \n", err)
		}
		delete(d.clientMap, key)
	}
	c, err = NewDeviceClient(info)
	d.Logger.Debugf("Creating new device client for key: %s", key)
	if err != nil {
		driver.Logger.Errorf("create device client failed. err:%v \n", err)
		return nil, err
	}
	d.clientMap[key] = c
	return c, nil
}

func (d *Driver) DisconnectDevice(deviceName string, protocols map[string]models.ProtocolProperties) error {
	d.Logger.Warn("Driver's DisconnectDevice function didn't implement")
	return nil
}

// lockAddress mark address is unavailable because real device handle one request at a time
func (d *Driver) lockAddress(address string) error {
	if d.stopped {
		return fmt.Errorf("service attempts to stop and unable to handle new request")
	}
	d.addressMutex.RLock()
	lock, ok := d.addressMap[address]
	d.addressMutex.RUnlock()
	if !ok {
		lock = make(chan bool, 1)
		d.addressMutex.Lock()
		d.addressMap[address] = lock
		d.addressMutex.Unlock()
	}

	// workingAddressCount used to check high-frequency command execution to avoid goroutine block
	count, ok := d.workingAddressCount[address]
	if !ok {
		// d.workingAddressCount[address] = 1
		v := atomic.Int32{}
		v.Store(1)
		d.workingAddressCount[address] = &v
	} else if count.Load() >= concurrentCommandLimit {
		errorMessage := fmt.Errorf("high-frequency command execution. There are %v commands with the same address in the queue", concurrentCommandLimit)
		d.Logger.Error(errorMessage.Error())
		return errorMessage
	} else {
		d.workingAddressCount[address].Add(1)
	}

	lock <- true

	return nil
}

// unlockAddress remove token after command finish
func (d *Driver) unlockAddress(address string) {
	d.addressMutex.RLock()
	lock := d.addressMap[address]
	d.addressMutex.RUnlock()

	d.workingAddressCount[address].Add(-1)
	<-lock
}

// lockableAddress return the lockable address according to the protocol
func (d *Driver) lockableAddress(info *ConnectionInfo) string {
	var address string
	if info.Protocol == ProtocolTCP {
		address = fmt.Sprintf("%s:%d", info.Address, info.Port)
	} else {
		address = info.Address
	}
	return address
}

func (d *Driver) HandleReadCommands(deviceName string, protocols map[string]models.ProtocolProperties, reqs []sdkModel.CommandRequest) (responses []*sdkModel.CommandValue, err error) {
	connectionInfo, err := createConnectionInfo(protocols)
	if err != nil {
		driver.Logger.Errorf("Fail to create read command connection info. err:%v \n", err)
		return responses, err
	}
	a := d.lockableAddress(connectionInfo)

	err = d.lockAddress(a)
	if err != nil {
		return responses, err
	}
	defer d.unlockAddress(a)

	responses = make([]*sdkModel.CommandValue, len(reqs))

	// create device client and open connection
	deviceClient, err := d.createDeviceClient(connectionInfo, false)

	if err != nil {
		return nil, err
	}

	driver.Logger.Debugf("key = %s,client = %+v", connectionInfo.String(), deviceClient)

	// handle command requests
	for i, req := range reqs {
		var attempts int
		var err error
		for attempts = 1; attempts <= maxRetries; attempts++ {
			d.Logger.Debugf("read_attempt#%d", attempts)
			var res *sdkModel.CommandValue
			res, err = handleReadCommandRequest(deviceClient, req)
			if err == nil {
				responses[i] = res
				break
			} else if errors.Is(err, io.EOF) {
				d.Logger.Errorf("handle read command request failed with EOF, retrying... attempt#%d, error: %v", attempts, err)
				time.Sleep(retryDelay)
				deviceClient, err = d.createDeviceClient(connectionInfo, true)
				if err != nil {
					d.Logger.Errorf("create device client failed, err: %v", err)
					break
				}
			} else {
				err := fmt.Errorf("handle read command request failed, error: %w", err)
				d.Logger.Errorf(err.Error())
				return nil, err
			}
		}
		if attempts > maxRetries {
			d.Logger.Errorf("handle read command request failed after %d attempts, error: %v", maxRetries, err)
			return nil, fmt.Errorf("failed to handle read command request after %d attempts: %w", maxRetries, err)
		}
		if responses[i] == nil {
			err = fmt.Errorf("handle read command request failed, response is nil, error: %w", err)
			d.Logger.Errorf(err.Error())
			return nil, err
		}
		driver.Logger.Debugf("Read command finished. Cmd:%v, %v \n", req.DeviceResourceName, responses[i])
	}
	driver.Logger.Debugf("get response %v", responses)
	return responses, nil
}

func handleReadCommandRequest(deviceClient DeviceClient, req sdkModel.CommandRequest) (*sdkModel.CommandValue, error) {
	var response []byte
	var result = &sdkModel.CommandValue{}
	var err error

	commandInfo, err := createCommandInfo(&req)
	if err != nil {
		return nil, err
	}

	response, err = deviceClient.GetValue(commandInfo)
	if err != nil {
		return result, err
	}

	result, err = TransformDataBytesToResult(&req, response, commandInfo)

	if err != nil {
		return result, err
	} else {
		driver.Logger.Infof("Read command finished. Cmd:%v, %v \n", req.DeviceResourceName, result)
	}

	return result, nil
}

func (d *Driver) HandleWriteCommands(deviceName string, protocols map[string]models.ProtocolProperties, reqs []sdkModel.CommandRequest, params []*sdkModel.CommandValue) error {
	connectionInfo, err := createConnectionInfo(protocols)
	if err != nil {
		driver.Logger.Errorf("Fail to create write command connection info. err:%v \n", err)
		return err
	}
	a := d.lockableAddress(connectionInfo)

	err = d.lockAddress(a)
	if err != nil {
		return err
	}
	defer d.unlockAddress(a)

	// create device client and open connection
	deviceClient, err := d.createDeviceClient(connectionInfo, false)
	if err != nil {
		return err
	}
	driver.Logger.Debugf("key = %s,client = %+v", connectionInfo.String(), deviceClient)

	errs := make([]error, 0)

	// handle command requests
	for i, req := range reqs {
		var attempts int
		var err error
		for attempts = 1; attempts <= maxRetries; attempts++ {
			d.Logger.Debugf("write_attempt#%d", attempts)
			err = handleWriteCommandRequest(deviceClient, req, params[i])
			if err == nil {
				break
			} else if errors.Is(err, io.EOF) {
				d.Logger.Errorf("handle write command request failed with EOF, retrying... attempt#%d, error: %v", attempts, err)
				time.Sleep(retryDelay)
				deviceClient, err = d.createDeviceClient(connectionInfo, true)
				if err != nil {
					d.Logger.Errorf("create device client failed, err: %v", err)
					errs = append(errs, err)
					// try to recreate the device client once more
					deviceClient, err = d.createDeviceClient(connectionInfo, false)
				}
			} else {
				d.Logger.Warnf("handle write command request failed, retrying... attempt#%d, error: %v", attempts, err)
			}
		}
		if attempts > maxRetries {
			d.Logger.Errorf("handle write command request failed after %d attempts, error: %v", maxRetries, err)
			errs = append(errs, fmt.Errorf("failed to handle write command request after %d attempts: %w", maxRetries, err))
		} else {
			driver.Logger.Debugf("Write command finished. Cmd:%v \n", req.DeviceResourceName)
		}
	}

	return errors.Join(errs...)
}

func handleWriteCommandRequest(deviceClient DeviceClient, req sdkModel.CommandRequest, param *sdkModel.CommandValue) error {
	var err error

	commandInfo, err := createCommandInfo(&req)
	if err != nil {
		return err
	}

	dataBytes, err := TransformCommandValueToDataBytes(commandInfo, param)
	if err != nil {
		return fmt.Errorf("transform command value failed, err: %v", err)
	}

	err = deviceClient.SetValue(commandInfo, dataBytes)
	if err != nil {
		return fmt.Errorf("handle write command request failed, err: %v", err)
	}

	driver.Logger.Infof("Write command finished. Cmd:%v \n", req.DeviceResourceName)
	return nil
}

func (d *Driver) Initialize(sdk interfaces.DeviceServiceSDK) error {
	d.Logger = sdk.LoggingClient()
	d.AsyncCh = sdk.AsyncValuesChannel()
	d.addressMap = make(map[string]chan bool)
	d.workingAddressCount = make(map[string]*atomic.Int32)
	d.clientMap = make(map[string]DeviceClient)
	return nil
}

func (d *Driver) Start() error {
	return nil
}

func (d *Driver) Stop(force bool) error {
	d.clientMutex.Lock()
	for key, client := range d.clientMap {
		err := client.CloseConnection()
		if err != nil {
			d.Logger.Errorf("device client closed,client key = %s,err = %v", key, err)
		}
	}
	d.clientMutex.Unlock()
	d.stopped = true
	if !force {
		d.waitAllCommandsToFinish()
	}
	for _, locked := range d.addressMap {
		close(locked)
	}
	return nil
}

// waitAllCommandsToFinish used to check and wait for the unfinished job
func (d *Driver) waitAllCommandsToFinish() {
loop:
	for {
		for _, count := range d.workingAddressCount {
			if count.Load() != 0 {
				// wait a moment and check again
				time.Sleep(time.Second * SERVICE_STOP_WAIT_TIME)
				continue loop
			}
		}
		break loop
	}
}

func (d *Driver) AddDevice(deviceName string, protocols map[string]models.ProtocolProperties, adminState models.AdminState) error {
	d.Logger.Debugf("Device %s is added", deviceName)
	return nil
}

func (d *Driver) UpdateDevice(deviceName string, protocols map[string]models.ProtocolProperties, adminState models.AdminState) error {
	d.Logger.Debugf("Device %s is updated", deviceName)
	return nil
}

func (d *Driver) RemoveDevice(deviceName string, protocols map[string]models.ProtocolProperties) error {
	d.Logger.Debugf("Device %s is removed", deviceName)
	return nil
}

func (d *Driver) Discover() error {
	return fmt.Errorf("driver's Discover function isn't implemented")
}

func (d *Driver) ValidateDevice(device models.Device) error {
	_, err := createConnectionInfo(device.Protocols)
	if err != nil {
		return fmt.Errorf("invalid protocol properties, %v", err)
	}
	return nil
}

func NewProtocolDriver() interfaces.ProtocolDriver {
	once.Do(func() {
		driver = new(Driver)
	})
	return driver
}
