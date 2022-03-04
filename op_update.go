package rotor

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// UpdateReturnValue
const (
	UpdateReturnValueNone       = dynamodb.ReturnValueNone
	UpdateReturnValueAllNew     = dynamodb.ReturnValueAllNew
	UpdateReturnValueAllOld     = dynamodb.ReturnValueAllOld
	UpdateReturnValueUpdatedNew = dynamodb.ReturnValueUpdatedNew
	UpdateReturnValueUpdatedOld = dynamodb.ReturnValueUpdatedOld
)

// UpdateOptions UpdateOptions
type UpdateOptions struct {
	builder *expression.Builder

	returnValue *string
}

func defaultUpdateOptions() *UpdateOptions {
	return &UpdateOptions{}
}

// UpdateOption UpdateOption
type UpdateOption func(options *UpdateOptions)

// UpdateCondition UpdateCondition
func UpdateCondition(condition expression.ConditionBuilder) UpdateOption {
	return func(options *UpdateOptions) {
		if options.builder == nil {
			builder := expression.NewBuilder()
			options.builder = &builder
		}
		*options.builder = options.builder.WithCondition(condition)
	}
}

// UpdateReturnValue UpdateReturnValue
func UpdateReturnValue(v string) UpdateOption {
	return func(options *UpdateOptions) {
		options.returnValue = aws.String(v)
	}
}

// UpdateOut update item
func (rs *Service) UpdateOut(ctx context.Context, key PrimaryKeyType, update expression.UpdateBuilder, out interface{}, opts ...UpdateOption) error {
	options := defaultUpdateOptions()
	options.returnValue = aws.String(UpdateReturnValueUpdatedNew)
	for _, opt := range opts {
		opt(options)
	}
	if aws.StringValue(options.returnValue) == UpdateReturnValueNone {
		return ErrReturnValue
	}
	var expr expression.Expression
	var err error
	if options.builder == nil {
		builder := expression.NewBuilder()
		options.builder = &builder
	}
	expr, err = options.builder.WithUpdate(update).Build()
	if err != nil {
		return err
	}
	ret, err := rs.dynamo.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName:                 rs.tableName,
		Key:                       key,
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              options.returnValue,
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

// Update update item
func (rs *Service) Update(ctx context.Context, key PrimaryKeyType, update expression.UpdateBuilder, opts ...UpdateOption) error {
	options := defaultUpdateOptions()
	options.returnValue = aws.String(UpdateReturnValueNone)
	for _, opt := range opts {
		opt(options)
	}
	if aws.StringValue(options.returnValue) != UpdateReturnValueNone {
		return ErrReturnValue
	}
	var expr expression.Expression
	var err error
	if options.builder == nil {
		builder := expression.NewBuilder()
		options.builder = &builder
	}
	expr, err = options.builder.WithUpdate(update).Build()
	if err != nil {
		return err
	}
	_, err = rs.dynamo.UpdateItemWithContext(ctx, &dynamodb.UpdateItemInput{
		TableName:                 rs.tableName,
		Key:                       key,
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              options.returnValue,
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

// UpdateBatch update items
func (rs *Service) UpdateBatch(ctx context.Context, keys []PrimaryKeyType, update expression.UpdateBuilder, opts ...UpdateOption) error {
	if len(keys) == 0 || len(keys) > maxWriteNum {
		return ErrInput
	}
	options := defaultUpdateOptions()
	for _, opt := range opts {
		opt(options)
	}
	var expr expression.Expression
	var err error
	if options.builder == nil {
		builder := expression.NewBuilder()
		options.builder = &builder
	}
	expr, err = options.builder.WithUpdate(update).Build()
	if err != nil {
		return err
	}
	inItems := make([]*dynamodb.TransactWriteItem, len(keys))
	for i, key := range keys {
		inItems[i] = &dynamodb.TransactWriteItem{
			Update: &dynamodb.Update{
				TableName:                 rs.tableName,
				Key:                       key,
				UpdateExpression:          expr.Update(),
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
