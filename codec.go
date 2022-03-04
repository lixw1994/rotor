package rotor

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Codec for ddb
// dynamodb 编解码的默认行为很容易产生误解
type Codec struct {
	Encoder *dynamodbattribute.Encoder
	Decoder *dynamodbattribute.Decoder
}

// NewCodec NewCodec
func NewCodec() Codec {
	return Codec{
		Encoder: dynamodbattribute.NewEncoder(
			func(e *dynamodbattribute.Encoder) {
				e.SupportJSONTags = false
				e.NullEmptyByteSlice = false
				e.NullEmptyString = false
				e.EnableEmptyCollections = true
			},
		),
		Decoder: dynamodbattribute.NewDecoder(func(d *dynamodbattribute.Decoder) {
			d.SupportJSONTags = false
			d.EnableEmptyCollections = true
		}),
	}
}

// Marshal will marshal a Go value type to an AttributeValue
// Marshal cannot represent cyclic data structures and will not handle them.
// Passing cyclic structures to Marshal will result in an infinite recursion.
func (codec Codec) Marshal(in interface{}) (*dynamodb.AttributeValue, error) {
	return codec.Encoder.Encode(in)
}

// MarshalMap is an alias for Marshal func which marshals Go value
// type to a map of AttributeValues.
//
// This is useful for DynamoDB APIs such as PutItem.
func (codec Codec) MarshalMap(in interface{}) (map[string]*dynamodb.AttributeValue, error) {
	av, err := codec.Encoder.Encode(in)
	if err != nil || av == nil || av.M == nil {
		return map[string]*dynamodb.AttributeValue{}, err
	}

	return av.M, nil
}

// MarshalList is an alias for Marshal func which marshals Go value
// type to a slice of AttributeValues.
func (codec Codec) MarshalList(in interface{}) ([]*dynamodb.AttributeValue, error) {
	av, err := codec.Encoder.Encode(in)
	if err != nil || av == nil || av.L == nil {
		return []*dynamodb.AttributeValue{}, err
	}

	return av.L, nil
}

// Unmarshal will unmarshal an AttributeValue into a Go value type
// The output value provided must be a non-nil pointer
func (codec Codec) Unmarshal(av *dynamodb.AttributeValue, out interface{}) error {
	return codec.Decoder.Decode(av, out)
}

// UnmarshalMap is an alias for Unmarshal which unmarshals from
// a map of AttributeValues.
//
// The output value provided must be a non-nil pointer
func (codec Codec) UnmarshalMap(m map[string]*dynamodb.AttributeValue, out interface{}) error {
	return codec.Decoder.Decode(&dynamodb.AttributeValue{M: m}, out)
}

// UnmarshalList is an alias for Unmarshal func which unmarshals
// a slice of AttributeValues.
//
// The output value provided must be a non-nil pointer
func (codec Codec) UnmarshalList(l []*dynamodb.AttributeValue, out interface{}) error {
	return codec.Decoder.Decode(&dynamodb.AttributeValue{L: l}, out)
}

// UnmarshalListOfMaps is an alias for Unmarshal func which unmarshals a
// slice of maps of attribute values.
//
// This is useful for when you need to unmarshal the Items from a DynamoDB
// Query API call.
//
// The output value provided must be a non-nil pointer
func (codec Codec) UnmarshalListOfMaps(l []map[string]*dynamodb.AttributeValue, out interface{}) error {
	items := make([]*dynamodb.AttributeValue, len(l))
	for i, m := range l {
		items[i] = &dynamodb.AttributeValue{M: m}
	}

	return codec.UnmarshalList(items, out)
}
