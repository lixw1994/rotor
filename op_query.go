package rotor

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// QuerySelectType
const (
	QuerySelectCount = "QuerySelectCount"
	QuerySelectASC   = "QuerySelectASC"
	QuerySelectDESC  = "QuerySelectDESC"
)

// QueryOptions QueryOptions
type QueryOptions struct {
	builder *expression.Builder

	consistentRead *bool
	selectType     *string
	indexName      *string
}

func defaultQueryOptions(keyCond expression.KeyConditionBuilder) *QueryOptions {
	builder := expression.NewBuilder().WithKeyCondition(keyCond)
	return &QueryOptions{
		builder: &builder,
	}
}

// QueryOption QueryOption
type QueryOption func(options *QueryOptions)

// QueryProjection QueryProjection
func QueryProjection(projection expression.ProjectionBuilder) QueryOption {
	return func(options *QueryOptions) {
		if options.builder == nil {
			builder := expression.NewBuilder()
			options.builder = &builder
		}
		*options.builder = options.builder.WithProjection(projection)
	}
}

// QueryFilter QueryFilter
func QueryFilter(filter expression.ConditionBuilder) QueryOption {
	return func(options *QueryOptions) {
		if options.builder == nil {
			builder := expression.NewBuilder()
			options.builder = &builder
		}
		*options.builder = options.builder.WithFilter(filter)
	}
}

// QueryConsistent QueryConsistent
func QueryConsistent(strong bool) QueryOption {
	return func(options *QueryOptions) {
		options.consistentRead = aws.Bool(strong)
	}
}

// QuerySelectType QuerySelectType
func QuerySelectType(v string) QueryOption {
	return func(options *QueryOptions) {
		options.selectType = aws.String(v)
	}
}

// QueryIndex QueryIndex
func QueryIndex(indexName string) QueryOption {
	return func(options *QueryOptions) {
		options.indexName = aws.String(indexName)
	}
}

// Query query items
// Item not found will return error
func (rs *Service) Query(ctx context.Context, keyCond expression.KeyConditionBuilder, out interface{}, opts ...QueryOption) error {
	options := defaultQueryOptions(keyCond)
	for _, opt := range opts {
		opt(options)
	}
	var expr expression.Expression
	var err error
	if options.builder == nil {
		builder := expression.NewBuilder()
		options.builder = &builder
	}
	expr, err = options.builder.WithKeyCondition(keyCond).Build()
	if err != nil {
		return err
	}
	input := &dynamodb.QueryInput{
		TableName:                 rs.tableName,
		IndexName:                 options.indexName,
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Condition(),
		ConsistentRead:            options.consistentRead,
	}
	if options.selectType != nil {
		switch *options.selectType {
		case QuerySelectASC:
			input.ScanIndexForward = aws.Bool(true)
			return rs.queryData(ctx, input, out)
		case QuerySelectDESC:
			input.ScanIndexForward = aws.Bool(false)
			return rs.queryData(ctx, input, out)
		case QuerySelectCount:
			input.Select = aws.String(dynamodb.SelectCount)
			c, err := rs.queryCount(ctx, input)
			if err != nil {
				return err
			}
			out = c
			return nil
		default:
			return ErrInput
		}
	}
	return rs.queryData(ctx, input, out)
}

func (rs *Service) queryData(ctx context.Context, input *dynamodb.QueryInput, out interface{}) error {
	allItems := []map[string]*dynamodb.AttributeValue{}
	err := rs.dynamo.QueryPagesWithContext(ctx, input, func(qo *dynamodb.QueryOutput, b bool) bool {
		allItems = append(allItems, qo.Items...)
		// TODO: 是否需要更好的处理
		if len(allItems) > maxReadNum {
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	return rs.codec.UnmarshalListOfMaps(allItems, out)
}

func (rs *Service) queryCount(ctx context.Context, input *dynamodb.QueryInput) (*int64, error) {
	ret, err := rs.dynamo.QueryWithContext(ctx, input)
	if err != nil {
		return nil, err
	}
	return ret.Count, nil
}
