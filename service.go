package rotor

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

const (
	maxReadNum  = 500
	maxWriteNum = 25
)

var (
	tablePK = "PK"
	tableSK = "SK"
)

// PrimaryKeyType dynamodb primary key
type PrimaryKeyType = map[string]*dynamodb.AttributeValue

// PrimaryKey PrimaryKey
func PrimaryKey(pk, sk string) PrimaryKeyType {
	return PrimaryKeyType{
		tablePK: {
			S: aws.String(pk),
		},
		tableSK: {
			S: aws.String(sk),
		},
	}
}

// ConditionItemNotExist ConditionItemNotExist
func ConditionItemNotExist() expression.ConditionBuilder {
	return expression.AttributeNotExists(expression.Name(tablePK))
}

// ConditionItemExist ConditionItemExist
func ConditionItemExist() expression.ConditionBuilder {
	return expression.AttributeExists(expression.Name(tableSK))
}

// BaseSchema base schema
type BaseSchema struct {
	PK         string     `dynamodbav:"PK"`
	SK         string     `dynamodbav:"SK"`
	Version    string     `dynamodbav:",omitempty"`
	CreateTime int64      `dynamodbav:",omitempty"`
	UpdateTime int64      `dynamodbav:",omitempty"`
	ExpireTime *time.Time `dynamodbav:",unixtime,omitempty"`
}

// Expired is expired
func (bs *BaseSchema) Expired() bool {
	if bs.ExpireTime == nil {
		return false
	}
	return bs.ExpireTime.After(time.Now())
}

// TTL get ttl
func (bs *BaseSchema) TTL() *time.Duration {
	if bs.ExpireTime == nil {
		return nil
	}
	d := bs.ExpireTime.Sub(time.Now())
	return &d
}

// SetTTL set ttl
func (bs *BaseSchema) SetTTL(d time.Duration) {
	bs.ExpireTime = aws.Time(time.Now().Add(d))
}

// Service dynamodb client service
type Service struct {
	dynamo    *dynamodb.DynamoDB
	codec     Codec
	tableName *string
}

// TableName TableName
func (rs *Service) TableName() string {
	return aws.StringValue(rs.tableName)
}

// New New service
func New(sess *session.Session, tableName string) *Service {
	return &Service{
		dynamo:    dynamodb.New(sess),
		codec:     NewCodec(),
		tableName: aws.String(tableName),
	}
}
