package rotor

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// GetOptions GetOptions
type GetOptions struct {
	builder        *expression.Builder
	consistentRead *bool
}

func defaultGetOptions() *GetOptions {
	return &GetOptions{}
}

// GetOption GetOption
type GetOption func(options *GetOptions)

// GetProjection GetProjection
func GetProjection(projection expression.ProjectionBuilder) GetOption {
	return func(options *GetOptions) {
		if options.builder == nil {
			builder := expression.NewBuilder()
			options.builder = &builder
		}
		*options.builder = options.builder.WithProjection(projection)
	}
}

// GetConsistent GetConsistent
func GetConsistent(strong bool) GetOption {
	return func(options *GetOptions) {
		options.consistentRead = aws.Bool(strong)
	}
}

// Get get item
// Item not found will return error
func (rs *Service) Get(ctx context.Context, key PrimaryKeyType, out interface{}, opts ...GetOption) error {
	options := defaultGetOptions()
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
	input := &dynamodb.GetItemInput{
		TableName:                rs.tableName,
		Key:                      key,
		ConsistentRead:           options.consistentRead,
		ProjectionExpression:     expr.Projection(),
		ExpressionAttributeNames: expr.Names(),
	}
	ret, err := rs.dynamo.GetItemWithContext(ctx, input)
	if err != nil {
		return err
	}
	if ret.Item == nil {
		return ErrItemNotFound
	}
	return rs.codec.UnmarshalMap(ret.Item, out)
}

// GetBatch get items
func (rs *Service) GetBatch(ctx context.Context, keys []PrimaryKeyType, out interface{}, opts ...GetOption) error {
	if len(keys) == 0 || len(keys) > maxReadNum {
		return ErrInput
	}
	options := defaultGetOptions()
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
	input := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{
			rs.TableName(): {
				Keys:                     keys,
				ProjectionExpression:     expr.Projection(),
				ExpressionAttributeNames: expr.Names(),
				ConsistentRead:           options.consistentRead,
			},
		},
	}
	var pageErr error
	allItems := []map[string]*dynamodb.AttributeValue{}
	err = rs.dynamo.BatchGetItemPagesWithContext(ctx, input, func(output *dynamodb.BatchGetItemOutput, b bool) bool {
		if len(output.UnprocessedKeys) > 0 {
			pageErr = ErrBatchGetPage
			return false
		}
		if items, ok := output.Responses[rs.TableName()]; ok {
			allItems = append(allItems, items...)
		}
		return true
	})
	if err != nil {
		return err
	}
	if pageErr != nil {
		return pageErr
	}
	return rs.codec.UnmarshalListOfMaps(allItems, out)
}
