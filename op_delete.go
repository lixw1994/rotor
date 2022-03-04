package rotor

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// DeleteOptions DeleteOptions
type DeleteOptions struct {
	builder *expression.Builder
}

func defaultDeleteOptions() *DeleteOptions {
	return &DeleteOptions{}
}

// DeleteOption DeleteOption
type DeleteOption func(options *DeleteOptions)

// DeleteCondition DeleteCondition
func DeleteCondition(condition expression.ConditionBuilder) DeleteOption {
	return func(options *DeleteOptions) {
		if options.builder == nil {
			builder := expression.NewBuilder()
			options.builder = &builder
		}
		*options.builder = options.builder.WithCondition(condition)
	}
}

// DeleteOut delete item
func (rs *Service) DeleteOut(ctx context.Context, key PrimaryKeyType, out interface{}, opts ...DeleteOption) error {
	options := defaultDeleteOptions()
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
	ret, err := rs.dynamo.DeleteItemWithContext(ctx, &dynamodb.DeleteItemInput{
		TableName:                 rs.tableName,
		Key:                       key,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              aws.String(dynamodb.ReturnValueAllOld),
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
	return rs.codec.UnmarshalMap(ret.Attributes, out)
}

// Delete delete item
func (rs *Service) Delete(ctx context.Context, key PrimaryKeyType, opts ...DeleteOption) error {
	options := defaultDeleteOptions()
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
	_, err = rs.dynamo.DeleteItemWithContext(ctx, &dynamodb.DeleteItemInput{
		TableName:                 rs.tableName,
		Key:                       key,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              aws.String(dynamodb.ReturnValueNone),
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

// DeleteBatch Delete items
func (rs *Service) DeleteBatch(ctx context.Context, keys []PrimaryKeyType, opts ...DeleteOption) error {
	if len(keys) == 0 || len(keys) > maxWriteNum {
		return ErrInput
	}
	options := defaultDeleteOptions()
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

	inItems := make([]*dynamodb.TransactWriteItem, len(keys))
	for i, key := range keys {
		inItems[i] = &dynamodb.TransactWriteItem{
			Delete: &dynamodb.Delete{
				TableName:                 rs.tableName,
				Key:                       key,
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
