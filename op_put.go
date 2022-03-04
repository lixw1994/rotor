package rotor

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// PutOptions PutOptions
type PutOptions struct {
	builder *expression.Builder
}

func defaultPutOptions() *PutOptions {
	return &PutOptions{}
}

// PutOption PutOption
type PutOption func(options *PutOptions)

// PutCondition PutCondition
func PutCondition(condition expression.ConditionBuilder) PutOption {
	return func(options *PutOptions) {
		if options.builder == nil {
			builder := expression.NewBuilder()
			options.builder = &builder
		}
		*options.builder = options.builder.WithCondition(condition)
	}
}

// PutOut put item
func (rs *Service) PutOut(ctx context.Context, in interface{}, out interface{}, opts ...PutOption) error {
	options := defaultPutOptions()
	for _, opt := range opts {
		opt(options)
	}
	var expr expression.Expression
	var err error
	if options.builder != nil {
		expr, err = options.builder.Build()
		if err != nil {
			return err
		}
	}
	item, err := rs.codec.MarshalMap(in)
	if err != nil {
		return err
	}
	input := &dynamodb.PutItemInput{
		TableName:                 rs.tableName,
		Item:                      item,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              aws.String(dynamodb.ReturnValueAllOld),
	}
	ret, err := rs.dynamo.PutItemWithContext(ctx, input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				return ErrConditionalCheck
			default:
				return err
			}
		}
		return err
	}
	return rs.codec.UnmarshalMap(ret.Attributes, out)
}

// Put put item
func (rs *Service) Put(ctx context.Context, in interface{}, opts ...PutOption) error {
	options := defaultPutOptions()
	for _, opt := range opts {
		opt(options)
	}
	var expr expression.Expression
	var err error
	if options.builder != nil {
		expr, err = options.builder.Build()
		if err != nil {
			return err
		}
	}
	item, err := rs.codec.MarshalMap(in)
	if err != nil {
		return err
	}
	input := &dynamodb.PutItemInput{
		TableName:                 rs.tableName,
		Item:                      item,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              aws.String(dynamodb.ReturnValueNone),
	}
	_, err = rs.dynamo.PutItemWithContext(ctx, input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				return ErrConditionalCheck
			default:
				return err
			}
		}
		return err
	}
	return nil
}

// PutIfNotExist put item if not exist
func (rs *Service) PutIfNotExist(ctx context.Context, in interface{}) error {
	return rs.Put(ctx, in,
		PutCondition(ConditionItemNotExist()),
	)
}

// PutBatch Put items
func (rs *Service) PutBatch(ctx context.Context, ins []interface{}, opts ...PutOption) error {
	if len(ins) == 0 || len(ins) > maxWriteNum {
		return ErrInput
	}
	options := defaultPutOptions()
	for _, opt := range opts {
		opt(options)
	}
	var expr expression.Expression
	var err error
	if options.builder != nil {
		expr, err = options.builder.Build()
		if err != nil {
			return err
		}
	}

	inItems := make([]*dynamodb.TransactWriteItem, len(ins))
	for i, in := range ins {
		item, err := rs.codec.MarshalMap(in)
		if err != nil {
			return err
		}
		inItems[i] = &dynamodb.TransactWriteItem{
			Put: &dynamodb.Put{
				TableName:                 rs.tableName,
				Item:                      item,
				ConditionExpression:       expr.Condition(),
				ExpressionAttributeNames:  expr.Names(),
				ExpressionAttributeValues: expr.Values(),
			},
		}
	}
	_, err = rs.dynamo.TransactWriteItemsWithContext(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: inItems,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeConditionalCheckFailedException:
				return ErrConditionalCheck
			default:
				return err
			}
		}
		return err
	}
	return nil
}
