package rotor

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// TransactUpdateItem TransactUpdateItem
type TransactUpdateItem struct {
	Key          PrimaryKeyType
	Builder      *expression.Builder
	checkKey     PrimaryKeyType
	checkBuilder *expression.Builder
}

// TransactDeleteItem TransactDeleteItem
type TransactDeleteItem struct {
	Key     PrimaryKeyType
	Builder *expression.Builder
}

// TransactPutItem TransactPutItem
type TransactPutItem struct {
	Item    interface{}
	Builder *expression.Builder
}

// TransactOptions TransactOptions
type TransactOptions struct {
	UpdateItems []TransactUpdateItem
	PutItems    []TransactPutItem
	DeleteItems []TransactDeleteItem
}

func defaultTransactOptions() *TransactOptions {
	return &TransactOptions{
		UpdateItems: []TransactUpdateItem{},
		PutItems:    []TransactPutItem{},
		DeleteItems: []TransactDeleteItem{},
	}
}

// TransactOption TransactOption
type TransactOption func(options *TransactOptions)

// Transact Transact
func (rs *Service) Transact(ctx context.Context, opts ...TransactOption) error {
	options := defaultTransactOptions()
	for _, opt := range opts {
		opt(options)
	}
	writeLen := len(options.UpdateItems) + len(options.PutItems) + len(options.DeleteItems)
	if writeLen == 0 {
		return ErrInput
	}
	if writeLen > maxWriteNum {
		return ErrInput
	}

	inItems := make([]*dynamodb.TransactWriteItem, writeLen)
	for i, update := range options.UpdateItems {
		var expr expression.Expression
		var err error
		if update.Builder != nil {
			expr, err = update.Builder.Build()
			if err != nil {
				return err
			}
		}
		inItems[i] = &dynamodb.TransactWriteItem{
			Update: &dynamodb.Update{
				TableName:                 rs.tableName,
				Key:                       update.Key,
				UpdateExpression:          expr.Update(),
				ConditionExpression:       expr.Condition(),
				ExpressionAttributeNames:  expr.Names(),
				ExpressionAttributeValues: expr.Values(),
			},
		}
	}
	for i, put := range options.PutItems {
		var expr expression.Expression
		var err error
		if put.Builder != nil {
			expr, err = put.Builder.Build()
			if err != nil {
				return err
			}
		}
		item, err := rs.codec.MarshalMap(put.Item)
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
	for i, delete := range options.DeleteItems {
		var expr expression.Expression
		var err error
		if delete.Builder != nil {
			expr, err = delete.Builder.Build()
			if err != nil {
				return err
			}
		}
		inItems[i] = &dynamodb.TransactWriteItem{
			Delete: &dynamodb.Delete{
				TableName:                 rs.tableName,
				Key:                       delete.Key,
				ConditionExpression:       expr.Condition(),
				ExpressionAttributeNames:  expr.Names(),
				ExpressionAttributeValues: expr.Values(),
			},
		}
	}

	_, err := rs.dynamo.TransactWriteItemsWithContext(ctx, &dynamodb.TransactWriteItemsInput{
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
