// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//	http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

// Package plugins contains functions that are useful across fluent bit plugins.
// This package will be imported by the CloudWatch Logs and Kinesis Data Streams plugins.

package cloudwatch

import (
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	jsoniter "github.com/json-iterator/go"
)

const (
	// See: http://docs.aws.amazon.com/AmazonCloudWatchLogs/latest/APIReference/API_PutLogEvents.html
	perEventBytes          = 26
	maximumBytesPerPut     = 1048576
	maximumLogEventsPerPut = 10000
)

type LogStream struct {
	logEvents         []*cloudwatchlogs.InputLogEvent
	currentByteLength int
	nextSequenceToken *string
	logStreamName     string
}

type OutputPlugin struct {
	region          string
	logGroupName    string
	logStreamPrefix string
	client          *cloudwatchlogs.CloudWatchLogs
	streams         map[string]*LogStream
}

func (output *OutputPlugin) AddEvent(tag string, record map[interface{}]interface{}, timestamp time.Time) error {
	data, retCode, err := output.processRecord(record)
	if err != nil {
		return retCode, err
	}

	event := string(data)
	if len(output.logEvents) == maximumLogEventsPerPut || (output.currentByteLength+cloudwatchLen(event)) >= maximumBytesPerPut {
		err = output.putLogEvents()
		if err != nil {
			return err
		}
	}

	output.logEvents = append(output.logEvents, &cloudwatchlogs.InputLogEvent{
		Message:   aws.String(event),
		Timestamp: aws.Int64(timestamp.Unix()),
	})
	output.currentByteLength += cloudwatchLen(event)
	return nil
}

func (output *OutputPlugin) getLogStream(tag string) (*LogStream, error) {
	// find log stream by tag
	stream, ok := output.streams[tag]
	if !ok {
		// stream doesn't exist, create it
		return output.createStream(output.logStreamPrefix + tag)
	}

	return stream, nil
}

func (output *OutputPlugin) createStream(name string) (*LogStream, err) {
	_, err := output.client.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(output.logGroupName),
		LogStreamName: aws.String(name),
	})

	return &LogStream{
		logStreamName:     name,
		logEvents:         make([]*cloudwatchlogs.InputLogEvent, 0, maximumLogEventsPerPut),
		nextSequenceToken: nil, // sequence token not required for a new log stream
	}, err
}

func (output *OutputPlugin) processRecord(record map[interface{}]interface{}) ([]byte, int, error) {
	var err error
	record, err = plugins.DecodeMap(record)
	if err != nil {
		logrus.Debugf("[cloudwatch] Failed to decode record: %v\n", record)
		return nil, fluentbit.FLB_ERROR, err
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	data, err := json.Marshal(record)
	if err != nil {
		logrus.Debugf("[cloudwatch] Failed to marshal record: %v\n", record)
		return nil, fluentbit.FLB_ERROR, err
	}

	return data, fluentbit.FLB_OK, nil
}

func (output *CloudWatchOutput) Flush() error {
	return output.putLogEvents()
}

func (output *CloudWatchOutput) putLogEvents() error {
	fmt.Println("Sending to CloudWatch")
	response, err := output.client.PutLogEvents(&cloudwatchlogs.PutLogEventsInput{
		LogEvents:     output.logEvents,
		LogGroupName:  aws.String(output.logGroup),
		LogStreamName: aws.String(output.logStream),
		SequenceToken: output.nextSequenceToken,
	})
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Printf("Sent %d events to CloudWatch\n", len(output.logEvents))

	output.nextSequenceToken = response.NextSequenceToken
	output.logEvents = output.logEvents[:0]
	output.currentByteLength = 0

	return nil
}

// effectiveLen counts the effective number of bytes in the string, after
// UTF-8 normalization.  UTF-8 normalization includes replacing bytes that do
// not constitute valid UTF-8 encoded Unicode codepoints with the Unicode
// replacement codepoint U+FFFD (a 3-byte UTF-8 sequence, represented in Go as
// utf8.RuneError)
func effectiveLen(line string) int {
	effectiveBytes := 0
	for _, rune := range line {
		effectiveBytes += utf8.RuneLen(rune)
	}
	return effectiveBytes
}

func cloudwatchLen(event string) int {
	return effectiveLen(event) + perEventBytes
}
